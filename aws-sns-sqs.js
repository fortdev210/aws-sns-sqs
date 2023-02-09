import AWS from "aws-sdk";
import dotenv from "dotenv";

dotenv.config();

AWS.config.update({
  region: process.env.AWS_REGION,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
});

const sns = new AWS.SNS({ apiVersion: "2010-03-31" });
const sqs = new AWS.SQS({ apiVersion: "2012-11-05" });

const snsTopicArn = process.env.SNS_TOPIC_ARN;

// Create the queue
async function createSQSQueue() {
  return new Promise((resolve, reject) => {
    const queueName = "sync-organization-sqs";
    const queueAttributes = {
      QueueName: queueName,
      Attributes: {
        ReceiveMessageWaitTimeSeconds: "20",
        MessageRetentionPeriod: "86400",
      },
    };
    sqs.createQueue(queueAttributes, (error, data) => {
      if (error) {
        console.error(`Error creating queue: ${error}`);
        reject(error);
      } else {
        console.log(`Successfully created queue:`, data);
        const sqsQueueUrl = data.QueueUrl;
        sqs.getQueueAttributes(
          { QueueUrl: data.QueueUrl, AttributeNames: ["QueueArn"] },
          (err, data) => {
            if (err) {
              console.error(err);
              return;
            }

            const queueArn = data.Attributes.QueueArn;
            resolve({ sqsQueueUrl: sqsQueueUrl, sqsQueueArn: queueArn });
          }
        );
      }
    });
  });
}

async function allowSqsToReceiveMsgFromSns(sqsQueueUrl, topicArn, sqsQueueArn) {
  const params = {
    QueueUrl: sqsQueueUrl,
    Attributes: {
      Policy: JSON.stringify({
        Version: "2012-10-17",
        Statement: [
          {
            Effect: "Allow",
            Principal: "*",
            Action: ["SQS:SendMessage"],
            Resource: sqsQueueArn,
            Condition: {
              ArnEquals: {
                "aws:SourceArn": topicArn,
              },
            },
          },
        ],
      }),
    },
  };

  return new Promise((resolve, reject) => {
    sqs.setQueueAttributes(params, function (err, data) {
      if (err) {
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
}

async function subscribeSqsToSns(topicArn, queueArn) {
  const subscribeParams = {
    Protocol: "sqs",
    TopicArn: topicArn,
    Endpoint: queueArn,
  };

  try {
    const subscribeResult = await sns.subscribe(subscribeParams).promise();
  } catch (error) {
    console.error(
      `Error subscribing SQS queue ${queueUrl} to SNS topic ${topicArn}:`,
      error
    );
  }
}

async function receiveMessages(queueUrl) {
  const params = {
    QueueUrl: queueUrl,
    MaxNumberOfMessages: 10,
    WaitTimeSeconds: 20,
  };
  const data = await sqs.receiveMessage(params).promise();

  if (data.Messages) {
    for (const message of data.Messages) {
      console.log("Message:", message.Body);
      // process the message
      // ...

      // after processing the message, delete it from the queue
      const deleteParams = {
        QueueUrl: queueUrl,
        ReceiptHandle: message.ReceiptHandle,
      };
      await sqs.deleteMessage(deleteParams).promise();
    }
  }

  // repeat the receiveMessages function to poll the queue continuously
  setTimeout(() => {
    receiveMessages(queueUrl);
  }, 1000);
}
async function main() {
  const { sqsQueueUrl, sqsQueueArn } = await createSQSQueue();
  await allowSqsToReceiveMsgFromSns(sqsQueueUrl, snsTopicArn, sqsQueueArn);
  await subscribeSqsToSns(snsTopicArn, sqsQueueArn);
  await receiveMessages(sqsQueueUrl);
}
main();
