// Command bootstrap creates the SNS topic, SQS queues and dead-letter queues.
//
// Locally it runs against Moto. In AWS this wiring belongs in Terraform; the
// command exists so the whole stack starts with a single `make up`.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/learnercys/event-pipeline/internal/awsx"
	"github.com/learnercys/event-pipeline/internal/config"
)

func main() {
	log := config.Logger("bootstrap")
	ctx := context.Background()

	cfg, err := awsx.Load(ctx)
	if err != nil {
		config.Fatal(err)
	}
	sqsc, snsc := awsx.SQS(cfg), awsx.SNS(cfg)

	topicName := config.String("TOPIC_NAME", "payment-events")
	visibility := config.Int("VISIBILITY_TIMEOUT_SECONDS", 30)
	maxReceives := config.Int("MAX_RECEIVE_COUNT", 5)

	// The emulator may still be starting; retry the first call for a while.
	var topic *sns.CreateTopicOutput
	for attempt := 1; ; attempt++ {
		topic, err = snsc.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String(topicName)})
		if err == nil {
			break
		}
		if attempt == 30 {
			config.Fatal(fmt.Errorf("create topic: %w", err))
		}
		time.Sleep(time.Second)
	}
	log.Info("topic ready", "arn", aws.ToString(topic.TopicArn))

	// Every consumer gets its own queue (fan-out) and its own DLQ, so a slow
	// or broken consumer never holds back the others.
	for _, name := range []string{"ledger-events", "analytics-events"} {
		dlqURL, dlqARN := createQueue(ctx, sqsc, name+"-dlq", map[string]string{
			"MessageRetentionPeriod": "1209600", // 14 days to investigate
		})
		redrive, _ := json.Marshal(map[string]string{
			"deadLetterTargetArn": dlqARN,
			"maxReceiveCount":     strconv.Itoa(maxReceives),
		})
		queueURL, queueARN := createQueue(ctx, sqsc, name, map[string]string{
			"VisibilityTimeout": strconv.Itoa(visibility),
			"RedrivePolicy":     string(redrive),
		})
		if _, err := snsc.Subscribe(ctx, &sns.SubscribeInput{
			TopicArn:   topic.TopicArn,
			Protocol:   aws.String("sqs"),
			Endpoint:   aws.String(queueARN),
			Attributes: map[string]string{"RawMessageDelivery": "true"},
		}); err != nil {
			config.Fatal(fmt.Errorf("subscribe %s: %w", name, err))
		}
		log.Info("queue ready", "queue", queueURL, "dlq", dlqURL,
			"visibility_timeout_s", visibility, "max_receive_count", maxReceives)
	}
}

func createQueue(ctx context.Context, c *sqs.Client, name string, attrs map[string]string) (url, arn string) {
	// Create without attributes, then set them: CreateQueue fails if the queue
	// already exists with different attributes, and this keeps reruns idempotent.
	out, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name)})
	if err != nil {
		config.Fatal(fmt.Errorf("create queue %s: %w", name, err))
	}
	if _, err := c.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
		QueueUrl: out.QueueUrl, Attributes: attrs,
	}); err != nil {
		config.Fatal(fmt.Errorf("set attributes on %s: %w", name, err))
	}
	got, err := c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       out.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		config.Fatal(fmt.Errorf("queue arn %s: %w", name, err))
	}
	return aws.ToString(out.QueueUrl), got.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]
}
