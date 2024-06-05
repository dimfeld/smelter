#!/usr/bin/env zx

function makeTaskDefinition(tag) {
  return {
    family: `smelter-example-task-${tag}`,
    taskRoleArn: 'arn:aws:iam::123123:role/fargate_smelter_example_task',
    executionRoleArn:
      'arn:aws:iam::123123:role/fargate_smelter_example_task_execution',
    networkMode: 'awsvpc',
    requiresCompatibilities: ['FARGATE'],
    cpu: '8 vCPU',
    memory: '32 GB',
    runtimePlatform: { cpuArchitecture: 'ARM64' },
    // The container with logging to S3
    containerDefinitions: [
      {
        name: 'log_router',
        essential: true,
        image: 'amazon/aws-for-fluent-bit:stable',
        firelensConfiguration: {
          type: 'fluentbit',
        },
        logConfiguration: {
          logDriver: 'awslogs',
          options: {
            'awslogs-group': 'firelens-container',
            'awslogs-region': 'us-west-2',
            'awslogs-create-group': 'true',
            'awslogs-stream-prefix': 'firelens',
          },
        },
        memoryReservation: 50,
      },
      {
        name: 'my-smelter-worker',
        image: `123123.dkr.ecr.us-west-2.amazonaws.com/my-smelter-worker:${tag}`,
        portMappings: [],
        essential: true,
        logConfiguration: {
          logDriver: 'awsfirelens',
          options: {
            Name: 's3',
            s3_key_format: '/%Y/%m/%Y%m%d-$TAG-%S',
            region: 'us-west-2',
            bucket: 'smelter-example-logs-123123',
            total_file_size: '10G',
            upload_timeout: '1m',
            retry_limit: '2',
          },
        },
      },
    ],
  };
}

let taskDefinition = makeTaskDefinition('dev');
$`aws ecs register-task-definition --cli-input-json ${JSON.stringify(taskDefinition)}`;
