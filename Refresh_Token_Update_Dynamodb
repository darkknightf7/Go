package main

import (
    "context"
    "encoding/json"
    "log"
    "os"
    "regexp"
    "strconv"
    "strings"
    "sync"
    "time"
    // AWS SDK for config and DynamoDB interactions
    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Loads ALL records into memory every run.
// We'll skip re-processed items if ttl_updated is already set.
// Now we track final counts of: revoked, over 30 days, expired,
// updated top records, updated TTLs, plus ttl_updated set count.

const (
    tableName        = "YourDynamoDBTable" // DynamoDB table name
    dataKey          = "data"              // Column name where JSON data is stored
    rkeyKey          = "rkey"              // Column name for secondary key
    ttlKey           = "ttl"              // Column name for time-to-live value
    revokedWord      = "\"revoked\":true" // String to identify revoked records
    updatedFlagAttr  = "ttl_updated" // Mark processed items
    updatedFlagValue = "true"
)
// Precompiled regex (not heavily used in current code)
var re = regexp.MustCompile(`[^a-zA-Z0-9]`)

// RecordData represents fields in the DynamoDB 'data' JSON
type RecordData struct {
    ExpiresAt int64  `json:"expiresAt"`  // when token expires
    AuthToken string `json:"authToken"` // used to identify top record
}

// Stats is concurrency-safe container for counters
type Stats struct {
    mu                sync.Mutex
    RevokedCount      int64
    Over30DaysCount   int64
    ExpiredCount      int64
    UpdatedTopRecords int64
    UpdatedTTLs       int64
    FlagSetCount   int64 // how many times we set ttl_updated
    NoTopRecordCount  int64 // new counter for no top record events
    FoundTopRecordCount  int64 // new counter for found top record events
}

// global stats object
var scriptStats Stats

func (s *Stats) IncRevoked() {
    s.mu.Lock()
    s.RevokedCount++
    s.mu.Unlock()
}
func (s *Stats) IncOver30Days() {
    s.mu.Lock()
    s.Over30DaysCount++
    s.mu.Unlock()
}
func (s *Stats) IncExpired() {
    s.mu.Lock()
    s.ExpiredCount++
    s.mu.Unlock()
}
func (s *Stats) IncUpdatedTop() {
    s.mu.Lock()
    s.UpdatedTopRecords++
    s.mu.Unlock()
}
func (s *Stats) IncUpdatedTTL() {
    s.mu.Lock()
    s.UpdatedTTLs++
    s.mu.Unlock()
}
func (s *Stats) IncFlagSet() {
    s.mu.Lock()
    s.FlagSetCount++
    s.mu.Unlock()
}
func (s *Stats) IncNoTopRecord() {
    s.mu.Lock()
    s.NoTopRecordCount++
    s.mu.Unlock()
}

func (s *Stats) IncFoundTopRecord() {
    s.mu.Lock()
    s.FoundTopRecordCount++
    s.mu.Unlock()
}


// groupKeyForItem merges large rkey with caret-based if JSON authToken matches substring
// 1. If rkey has '^', use substring before '^'
// 2. Otherwise, parse JSON's authToken if possible
// 3. Else fallback to entire rkey
func groupKeyForItem(item map[string]types.AttributeValue) string {
    rkeyVal, ok := item[rkeyKey].(*types.AttributeValueMemberS)
    if !ok {
        return ""
    }
    rkey := rkeyVal.Value
    // If caret found, use substring before caret
    if idx := strings.Index(rkey, "^"); idx >= 0 {
        return rkey[:idx]
    }
    // No caret? Attempt to parse data's authToken
    dataVal := getDataValue(item)
    var rd RecordData
    if err := json.Unmarshal([]byte(dataVal), &rd); err == nil && rd.AuthToken != "" {
        return rd.AuthToken
    }
    // If all else fails, use entire rkey
    return rkey
}

func main() {
    // Set up logging
    logFile, err := os.OpenFile("update.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        log.Fatalf("failed to open log file: %v", err)
    }
    defer logFile.Close()

    errorLogFile, err := os.OpenFile("error.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        log.Fatalf("failed to open error log file: %v", err)
    }
    defer errorLogFile.Close()
    // Set normal logs to update.log, errors to error.log
    log.SetOutput(logFile)
    errorLogger := log.New(errorLogFile, "ERROR: ", log.LstdFlags)
    // Load AWS config (region, credentials, etc.)
    cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-east-1"))
    if err != nil {
        errorLogger.Fatalf(`{"level":"error","event":"aws_config","error":"%v"}`, err)
    }
    // Create DynamoDB client
    dynamoClient := dynamodb.NewFromConfig(cfg)
    
     // Quick connectivity check
     _, err = dynamoClient.ListTables(context.TODO(), &dynamodb.ListTablesInput{})
     if err != nil {
         errorLogger.Fatalf(`{"level":"error","event":"dynamodb_connect","error":"%v"}`, err)
     }

    // Approx itemCount
    tableDesc, err := dynamoClient.DescribeTable(context.TODO(), &dynamodb.DescribeTableInput{
        TableName: aws.String(tableName),
    })
    if err != nil {
        errorLogger.Fatalf(`{"level":"error","event":"describe_table","error":"%v"}`, err)
    }
    var totalCount int64
    if tableDesc.Table != nil && tableDesc.Table.ItemCount != nil {
        totalCount = *tableDesc.Table.ItemCount
    }
    log.Printf(`{"level":"info","event":"approx_count","message":"Approx itemCount=%d from DescribeTable"}`, totalCount)

    // We'll store ALL records in memory
    var allRecords []map[string]types.AttributeValue

    startTime := time.Now()
    var scannedCount int64

    // Just do a simple loop scanning until no more pages
    scanInput := &dynamodb.ScanInput{
        TableName: aws.String(tableName),
        Limit:     aws.Int32(100000),
    }
    // Repeatedly scan until no more pages
    for {
        result, err := dynamoClient.Scan(context.TODO(), scanInput)
        if err != nil {
            errorLogger.Fatalf(`{"level":"error","event":"scan","error":"%v"}`, err)
        }
        pageCount := int64(len(result.Items))
        scannedCount += pageCount

        // Accumulate these items => big in-memory usage
        allRecords = append(allRecords, result.Items...)

        // progress logging
        elapsedMin := time.Since(startTime).Minutes()
        if totalCount > 0 {
            pct := float64(scannedCount) / float64(totalCount) * 100.0
            // clamp at 100%
            if pct > 100.0 {
            pct = 100.0
            }
            rpm := float64(scannedCount) / elapsedMin
            remain := float64(totalCount - scannedCount)
            estRemainMin := 0.0
            if rpm > 0 {
                estRemainMin = remain / rpm
            }
            log.Printf(
                `{"level":"info","event":"progress","scannedSoFar":%d,"totalCount":%d,"pctComplete":%.2f,"rpm":%.1f,"estRemainMin":%.1f}`,
                scannedCount, totalCount, pct, rpm, estRemainMin,
            )
        } else {
            log.Printf(`{"level":"info","event":"progress","scannedSoFar":%d}`, scannedCount)
        }

        if result.LastEvaluatedKey == nil {
            // done scanning
            break
        }
        scanInput.ExclusiveStartKey = result.LastEvaluatedKey
    }

    elapsedSec := time.Since(startTime).Seconds()
     // Log how many records we got
    log.Printf(`{"level":"info","event":"scan_complete","scannedTotal":%d,"elapsedSec":%.2f,"message":"All rows loaded into memory"}`,
        scannedCount, elapsedSec,
    )

    // // Now group + process all
    // doGroupingAndProcessing(dynamoClient, allRecords, errorLogger)
    // Group and process all records; capture the total token group count.
    tokenGroupCount := doGroupingAndProcessing(dynamoClient, allRecords, errorLogger)

    // Final summary at the end
    scriptStats.mu.Lock()
    revoked := scriptStats.RevokedCount
    over30 := scriptStats.Over30DaysCount
    expired := scriptStats.ExpiredCount
    updatedTop := scriptStats.UpdatedTopRecords
    updatedTTLs := scriptStats.UpdatedTTLs
    flagsSet := scriptStats.FlagSetCount
    noTopRec   := scriptStats.NoTopRecordCount
    foundTopRec := scriptStats.FoundTopRecordCount
    scriptStats.mu.Unlock()
    completionTimeSec := time.Since(startTime).Seconds()

    log.Printf(`{"level":"info","event":"script_complete","total_count":%d,"total_token_groups":%d,"total_revoked":%d,"total_records_over_30_days":%d,"total_expired":%d,"total_updated_top_records":%d,"total_updated_ttls":%d,"total__updated_ttl_flags":%d,"total_no_top_records":%d,"total_found_top_records":%d, "completion_time":%.2f}`,
        scannedCount,
        tokenGroupCount,
        revoked,
        over30,
        expired,
        updatedTop,
        updatedTTLs,
        flagsSet,
        noTopRec,
        foundTopRec,
        completionTimeSec,
    )
}

func doGroupingAndProcessing(client *dynamodb.Client, allRecords []map[string]types.AttributeValue, errorLogger *log.Logger) int64 {    
    groups := make(map[string][]map[string]types.AttributeValue)

    for _, item := range allRecords {
        // If item already has ttl_updated => skip it entirely
        if _, updated := item[updatedFlagAttr]; updated {
            continue
        }

        gk := groupKeyForItem(item)
        if gk == "" {
            continue
        }
        groups[gk] = append(groups[gk], item)
    }
    // Convert grouped map to slice of slices for parallel processing
    var groupedData [][]map[string]types.AttributeValue
    for _, g := range groups {
        groupedData = append(groupedData, g)
    }
    // Process each group concurrently
    var wg sync.WaitGroup
    for _, grp := range groupedData {
        wg.Add(1)
        go func(groupItems []map[string]types.AttributeValue) {
            defer func() {
                if r := recover(); r != nil {
                    errorLogger.Printf(`{"level":"error","event":"panic_recovered","panic":"%v"}`, r)
                }
                wg.Done()
            }()
            processGroup(client, groupItems, errorLogger)
        }(grp)
    }
    wg.Wait()
    // Return the count of unique token groups
    return int64(len(groups))
}

func getRkey(item map[string]types.AttributeValue) string {
    if v, ok := item[rkeyKey].(*types.AttributeValueMemberS); ok {
        return v.Value
    }
    return ""
}
// processGroup looks for a 'top record' in the group, checks if it's valid, and updates
func processGroup(client *dynamodb.Client, group []map[string]types.AttributeValue, errorLogger *log.Logger) {
    if len(group) == 0 {
        return
    }

    cleanedGroup := make([]map[string]types.AttributeValue, 0, len(group))
    for _, item := range group {
        dataVal := getDataValue(item)
        if strings.Contains(dataVal, revokedWord) {
            scriptStats.IncRevoked()
            log.Printf(`{"level":"info","event":"skip_item","rkey":"%s","reason":"revoked","message":"Skipping single revoked item"}`, getRkey(item))
            continue
        }
        cleanedGroup = append(cleanedGroup, item)
    }
    if len(cleanedGroup) == 0 {
        log.Printf(`{"level":"info","event":"all_revoked","message":"All items revoked; skipping group"}`)
        return
    }
    group = cleanedGroup

    base := groupKeyForItem(group[0])
    // Identify top record => item whose authToken == the group key
    topIndex := -1
    var topExpires int64

    for i, itm := range group {
        dataVal := getDataValue(itm)
        var rd RecordData
        if err := json.Unmarshal([]byte(dataVal), &rd); err != nil {
            errorLogger.Printf(`{"level":"error","event":"unmarshal_failure","error":"%v"}`, err)
            continue
        }
        if rd.AuthToken == base {
            topIndex = i
            topExpires = rd.ExpiresAt
            log.Printf(`{"level":"info","event":"found_top_record","rkey":"%s","authToken":"%s","expiresAt":%d}`, getRkey(itm), rd.AuthToken, rd.ExpiresAt)
            scriptStats.IncFoundTopRecord() // Increment found top record counter
            break
        }
    }
    // If no top record found => do nothing
    if topIndex == -1 {
        log.Printf(`{"level":"info","event":"no_top_record", "rkey":"%s", "message":"No top record found, no action taken"}`,getRkey(group[0]))
        scriptStats.IncNoTopRecord() // Increment the no_top_record counter
        return
    }
    // Compare top record's expiresAt vs current time
    nowMs := time.Now().UnixNano() / int64(time.Millisecond)
    if topExpires < nowMs {
        scriptStats.IncExpired()
        log.Printf(`{"level":"info","event":"skip_group","rkey":"%s","reason":"expired","topRecordExpires":%d,"now":%d}`, getRkey(group[topIndex]), topExpires, nowMs)
        return
    }
    thirtyDaysMs := nowMs + (30 * 24 * 60 * 60 * 1000)
    if topExpires >= thirtyDaysMs {
        scriptStats.IncOver30Days()
        log.Printf(`{"level":"info","event":"skip_group","rkey":"%s","reason":"over_30_days","topRecordExpires":%d,"threshold":%d}`, getRkey(group[topIndex]), topExpires, thirtyDaysMs)
        return
    }
    // Otherwise => set top record to 10-year future, update TTL for entire group
    tenYearsFuture := nowMs + (10 * 365 * 24 * 60 * 60 * 1000)
    log.Printf(`{"level":"info","event":"update_group","rkey":"%s","oldExpires":%d,"newExpires":%d}`, getRkey(group[topIndex]), topExpires, tenYearsFuture)

    for i, itm := range group {
        if i == topIndex {
            log.Printf(`{"level":"info","event":"update_top_record","rkey":"%s","expiresAt":%d}`, getRkey(itm), tenYearsFuture)
            scriptStats.IncUpdatedTop()
            updateTopRecord(client, itm, tenYearsFuture, errorLogger)
        } else {
            log.Printf(`{"level":"info","event":"update_ttl_only","rkey":"%s","expiresAt":%d}`, getRkey(itm), tenYearsFuture)
            scriptStats.IncUpdatedTTL()
            updateTTLOnly(client, itm, tenYearsFuture, errorLogger)
        }
    }
}

// updateTTLOnly sets the TTL field in DynamoDB, leaving 'data' unchanged
func updateTTLOnly(client *dynamodb.Client, item map[string]types.AttributeValue, newExpirationMs int64, errorLogger *log.Logger) {
    newExpirationSec := newExpirationMs / 1000
    updateInput := &dynamodb.UpdateItemInput{
        TableName: aws.String(tableName),
        Key: map[string]types.AttributeValue{
            "hkey": item["hkey"],
            "rkey": item["rkey"],
        },
        // also set ttl_updated = 'true'
        UpdateExpression: aws.String("SET #ttl = :newTTL, #flag = :flagVal"),
        ExpressionAttributeNames: map[string]string{
            "#ttl":  ttlKey,
            "#flag": updatedFlagAttr,
        },
        ExpressionAttributeValues: map[string]types.AttributeValue{
            ":newTTL": &types.AttributeValueMemberN{
                Value: strconv.FormatInt(newExpirationSec, 10),
            },
            ":flagVal": &types.AttributeValueMemberS{
                Value: updatedFlagValue,
            },
        },
    }

    _, err := client.UpdateItem(context.TODO(), updateInput)
    if err != nil {
        errorLogger.Printf(`{"level":"error","event":"ttl_update_failure","rkey":"%s","error":"%v"}`, getRkey(item), err)
    } else {
        scriptStats.IncFlagSet()
    }
}

// updateTopRecord updates 'expiresAt' in JSON + TTL field
func updateTopRecord(client *dynamodb.Client, item map[string]types.AttributeValue, newExpirationMs int64, errorLogger *log.Logger) {
    dataVal := getDataValue(item)
    var dataMap map[string]interface{}
    if err := json.Unmarshal([]byte(dataVal), &dataMap); err != nil {
        errorLogger.Printf(`{"level":"error","event":"unmarshal_failure","error":"%v"}`, err)
        return
    }

    dataMap["expiresAt"] = newExpirationMs
    updatedJSON, err := json.Marshal(dataMap)
    if err != nil {
        errorLogger.Printf(`{"level":"error","event":"marshal_failure","error":"%v"}`, err)
        return
    }
    // TTL must be in seconds
    newExpirationSec := newExpirationMs / 1000
    updateInput := &dynamodb.UpdateItemInput{
        TableName: aws.String(tableName),
        Key: map[string]types.AttributeValue{
            "hkey": item["hkey"],
            "rkey": item["rkey"],
        },
        // also set ttl_updated = '1'
        UpdateExpression: aws.String("SET #ttl = :newTTL, #data = :newData, #flag = :flagVal"),
        ExpressionAttributeNames: map[string]string{
            "#ttl":  ttlKey,
            "#data": dataKey,
            "#flag": updatedFlagAttr,
        },
        ExpressionAttributeValues: map[string]types.AttributeValue{
            ":newTTL": &types.AttributeValueMemberN{
                Value: strconv.FormatInt(newExpirationSec, 10),
            },
            ":newData": &types.AttributeValueMemberS{
                Value: string(updatedJSON),
            },
            ":flagVal": &types.AttributeValueMemberS{
                Value: updatedFlagValue,
            },
        },
    }

    _, err = client.UpdateItem(context.TODO(), updateInput)
    if err != nil {
        errorLogger.Printf(`{"level":"error","event":"top_record_update_failure","rkey":"%s","error":"%v"}`, getRkey(item), err)
    } else {
        scriptStats.IncFlagSet()
    }
}

// getDataValue pulls the 'data' field as a string from the item
func getDataValue(item map[string]types.AttributeValue) string {
    if v, ok := item["data"].(*types.AttributeValueMemberS); ok {
        return v.Value
    }
    return ""
}
