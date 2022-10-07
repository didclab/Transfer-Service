variable "region" {
    description = "Region to deploy resources in"
    default = "us-east-2"
}
variable "image_id" {
    description = "Image ID to use with launch template for ASG"
}