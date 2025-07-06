package fakedynamo_test

import (
	"cmp"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DMRobertson/fakedynamo"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/stretchr/testify/assert"
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

func TestMain(m *testing.M) {
	retval := m.Run()

	if dynamodbSession != nil {
		var wg sync.WaitGroup

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
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, err := db.DeleteTable(&dynamodb.DeleteTableInput{
						TableName: tableName,
					})
					if err != nil {
						panic(err)
					}
				}()
			}
		}
	}

	os.Exit(retval)
}

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

func ptr[T any](v T) *T {
	return &v
}

func val[T any](p *T) T {
	return *p
}

func comparePtr[T cmp.Ordered](a, b *T) int {
	return cmp.Compare(*a, *b)
}

func assertErrorContains(t *testing.T, err error, needles ...string) {
	t.Helper()
	if !assert.Error(t, err) {
		return
	}
	for _, needle := range needles {
		assert.ErrorContains(t, err, needle)
	}
}
