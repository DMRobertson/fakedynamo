package fakedynamo_test

import (
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DMRobertson/fakedynamo"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

var (
	dynamodbSession *session.Session
	nonceCounter    atomic.Int64
)

func init() {
	nonceCounter.Store(time.Now().Unix())

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

// makeTestDB produces a new [dynamodbiface.DynamoDBAPI] implementation ready
// for testing. If testing against DynamoDB local, we attempt to automatically
// delete the table at the end of the test.
func makeTestDB(t *testing.T) dynamodbiface.DynamoDBAPI {
	t.Helper()
	if dynamodbSession != nil {
		return autocleaningDynamoDB{
			DynamoDBAPI: dynamodb.New(dynamodbSession),
			t:           t,
		}
	}
	return fakedynamo.NewDB()
}

// autocleaningDynamoDB is a wrapper which calls [dynamodbiface.DynamoDBAPI.DeleteTable]
// at test cleanup to remove any tables that were created by tests.
type autocleaningDynamoDB struct {
	dynamodbiface.DynamoDBAPI
	t *testing.T
}

func (db autocleaningDynamoDB) CreateTable(input *dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error) {
	db.t.Cleanup(func() {
		_, _ = db.DeleteTable(&dynamodb.DeleteTableInput{
			TableName: input.TableName,
		})
	})
	return db.DynamoDBAPI.CreateTable(input)
}

func nonce() string {
	value := nonceCounter.Add(1)
	return strconv.Itoa(int(value))
}
