terraform {
  # 1.10.0 is the floor: backend.example.hcl (and the real deployed
  # backend) use the S3 backend's native locking (use_lockfile), which
  # 1.8.0 does not support - a real `tofu init -backend-config=...` on
  # 1.8.0 fails on that argument, even though `make verify-tofu`'s
  # `-backend=false` never exercises it. CI and local dev are tested
  # against 1.12.6 specifically (see .github/workflows/verify.yml).
  required_version = ">= 1.10.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.33"
    }
  }

  backend "s3" {}
}
