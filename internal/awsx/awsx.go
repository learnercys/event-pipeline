// Package awsx builds AWS clients that work against both real AWS and a local
// emulator. Set AWS_ENDPOINT_URL to point every client at the emulator.
package awsx

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// Load reads the standard AWS configuration (env vars, profiles, IAM roles).
// The SDK also honours AWS_ENDPOINT_URL, which is how local runs reach Moto.
func Load(ctx context.Context) (aws.Config, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return aws.Config{}, fmt.Errorf("load aws config: %w", err)
	}
	return cfg, nil
}

// SQS returns an SQS client.
func SQS(cfg aws.Config) *sqs.Client { return sqs.NewFromConfig(cfg) }

// SNS returns an SNS client.
func SNS(cfg aws.Config) *sns.Client { return sns.NewFromConfig(cfg) }
