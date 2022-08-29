# Log group for ecs cluster for monitoring
resource "aws_cloudwatch_log_group" "ods_cluster" {
    name = "ods_cluster"
}

# Launch template to use to create instance and add to ODS ASG
resource "aws_launch_template" "ods_lt" {
    name_prefix   = "ods"
    image_id      = var.imageID
    instance_type = "t2.medium"
}
# ASG to use with ECS capacity providers
resource "aws_autoscaling_group" "ods_ecs_asg" {
    availability_zones = ["us-east-2a","us-east-2b","us-east-2c"]
    desired_capacity = 1
    max_size = 10
    min_size = 1
    launch_template {
        id      = aws_launch_template.ods_lt.id
        version = "$Latest"
    }
    lifecycle {
        ignore_changes = [
            desired_capacity
        ]
    }
}
# TODO: capacity providers, task definitions, services

resource "aws_ecs_cluster" "ods" {
    name = "ods_services"
    setting {
        name = "containerInsights"
        value = "Enabled"
    }
    configuration {
        execute_command_configuration {
            logging = "DEFAULT"
            log_configuration {
                cloud_watch_log_group_name = aws_cloudwatch_log_group.ods_cluster.name
            }
        }
    }
}