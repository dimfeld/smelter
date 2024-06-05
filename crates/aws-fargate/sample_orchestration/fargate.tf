variable "aws_account_id" {
  default = "123123"
}

variable smelter_example_cidr {
  default = "173.22.0.0/16"
}

variable aws_region {
  default = "us-west-2"
}

variable az {
  default = "us-west-2c"
}

resource "aws_vpc" "smelter_example" {
  cidr_block = var.smelter_example_cidr
  tags = {
    Name = "smelter_example"
  }
}

resource "aws_subnet" "smelter_example" {
  vpc_id = aws_vpc.smelter_example.id
  cidr_block = var.smelter_example_cidr
  map_public_ip_on_launch = true
  availability_zone = var.az
  tags = {
    Name = "smelter_example"
  }
}

resource "aws_internet_gateway" "smelter_example" {
  vpc_id = aws_vpc.smelter_example.id

  tags = {
    Name = "smelter_example"
  }
}

resource "aws_route_table" "smelter_example" {
  vpc_id = aws_vpc.smelter_example.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.smelter_example.id
  }

  tags = {
    Name = "smelter_example"
  }
}

resource "aws_vpc_endpoint" "smelter_example_s3" {
  vpc_id       = aws_vpc.smelter_example.id
  service_name = "com.amazonaws.us-west-2.s3"

  tags = {
    Name = "smelter_example"
  }
}

resource "aws_vpc_endpoint_route_table_association" "smelter_example_s3" {
  vpc_endpoint_id = aws_vpc_endpoint.smelter_example_s3.id
  route_table_id = aws_route_table.smelter_example.id
}

resource "aws_main_route_table_association" "smelter_example" {
  vpc_id = aws_vpc.smelter_example.id
  route_table_id = aws_route_table.smelter_example.id
}

resource "aws_ecs_cluster" "smelter_example" {
  name = "smelter_example"
}

resource "aws_s3_bucket" "smelter_example_logs" {
  bucket = "smelter-example-logs-123123"
}

resource "aws_s3_bucket_lifecycle_configuration" "smelter_example_logs" {
  bucket = aws_s3_bucket.smelter_example_logs.id
  rule {
    id = "delete-old-logs"
    expiration {
      days = 90
    }
    status = "Enabled"
  }
}

data "aws_iam_policy_document" "ecs_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    effect = "Allow"
    principals {
      type = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }

    condition {
      test = "ArnLike"
      variable = "aws:SourceArn"
      values = [
        format("arn:aws:ecs:%s:%s:*", var.aws_region, var.aws_account_id)
      ]
    }

    condition {
      test = "StringEquals"
      variable = "aws:SourceAccount"
      values = [
        var.aws_account_id
      ]
    }

  }
}

resource "aws_iam_role" "smelter_example_task_execution_role" {
  name = "fargate_smelter_example_task_execution"

  assume_role_policy = data.aws_iam_policy_document.ecs_assume_role.json

  inline_policy {
    name = "s3_put"
    policy = jsonencode({
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Action": [
             "s3:*"
          ],
          "Resource": [
            "arn:aws:s3:::smelter-example-logs",
            "arn:aws:s3:::smelter-example-logs/*"
           ]
        },
        {
          "Effect": "Allow",
          "Action": [
            "ecr:GetAuthorizationToken",
            "logs:CreateLogGroup",
            "logs:CreateLogStream",
            "logs:PutLogEvents"
          ],
          "Resource": "*"
        },
        {
          "Effect": "Allow",
          "Action": [
            "ecr:BatchCheckLayerAvailability",
            "ecr:BatchGetImage",
            "ecr:DescribeImages",
            "ecr:DescribeRepositories",
            "ecr:GetDownloadUrlForLayer",
            "ecr:GetRepositoryPolicy",
            "ecr:Images"
          ],
          "Resource": [
            format("arn:aws:ecr:%s:%s:repository/*", var.aws_region, var.aws_account_id)
          ]
        }
      ]
    })
  }
}

resource "aws_iam_role" "smelter_example_task_role" {
  name = "fargate_smelter_example_task"

  assume_role_policy = data.aws_iam_policy_document.ecs_assume_role.json

  inline_policy {
    name = "s3_put"
    policy = jsonencode({
      "Version": "2012-10-17",
      "Statement": [{
        "Effect": "Allow",
        "Action": [
          "s3:*"
        ],
        "Resource": "*"
      }]
    })
  }
}

output "smelter_example_task_role_arn" {
  value = aws_iam_role.smelter_example_task_role.arn
}

output "smelter_example_task_execution_role_arn" {
  value = aws_iam_role.smelter_example_task_execution_role.arn
}
