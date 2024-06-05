set -exuo pipefail
TASK_DEF_ARN="arn:aws:ecs:us-west-2:123123:task-definition/smelter-example-task-dev"


aws ecs run-task \
  --network 'awsvpcConfiguration={subnets=[subnet-THE_SUBNET_ID],assignPublicIp=ENABLED}' \
  --cluster pipeline \
  --launch-type FARGATE \
  --task-definition ${TASK_DEF_ARN}
