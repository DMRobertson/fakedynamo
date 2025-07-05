package fakedynamo_test

import (
	"cmp"
	"os"

	"github.com/DMRobertson/fakedynamo"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

var dynamodbSession *session.Session

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

func makeTestDB() dynamodbiface.DynamoDBAPI {
	if dynamodbSession != nil {
		return dynamodb.New(dynamodbSession)
	}
	return fakedynamo.NewDB()
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
