resource "aws_ses_email_identity" "notifications" {
  count = var.ses_email_identity == "" ? 0 : 1

  email = var.ses_email_identity
}
