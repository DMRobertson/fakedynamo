package fakedynamo_test

import (
	"cmp"
	"os"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/DMRobertson/fakedynamo"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

var (
	dynamodbSession *session.Session
	nonceCounter    atomic.Uint64
)

func init() {
	endpoint := os.Getenv("DYNAMODB_ENDPOINT")
	if endpoint != "" {
		dynamodbSession = session.Must(session.NewSession(
			&aws.Config{
				Region:      aws.String("eu-west-2"),
				Credentials: credentials.NewStaticCredentials("fakeaccesskey", "fake-access-secret", ""),
				Endpoint:    aws.String(endpoint),
			}))
	}
}

func TestMain(m *testing.M) {
	retval := m.Run()

	if dynamodbSession != nil {
		db := dynamodb.New(dynamodbSession)
		for {
			result, err := db.ListTables(&dynamodb.ListTablesInput{})
			if err != nil {
				panic(err)
			}
			if len(result.TableNames) == 0 {
				break
			}
			for _, tableName := range result.TableNames {
				_, err := db.DeleteTable(&dynamodb.DeleteTableInput{
					TableName: tableName,
				})
				if err != nil {
					panic(err)
				}
			}
		}
	}

	os.Exit(retval)
}

func makeTestDB() dynamodbiface.DynamoDBAPI {
	if dynamodbSession != nil {
		return dynamodb.New(dynamodbSession)
	}
	return fakedynamo.NewDB()
}

func nonce() string {
	value := nonceCounter.Add(1)
	return strconv.Itoa(int(value))
}

func ptr[T any](v T) *T {
	return &v
}

func val[T any](p *T) T {
	return *p
}

func comparePtr[T cmp.Ordered](a, b *T) int {
	return cmp.Compare(*a, *b)
}
