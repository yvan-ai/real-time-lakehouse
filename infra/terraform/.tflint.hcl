# Shared tflint config for both Terraform roots (local/ and aws/).
# CI: tflint --config "$(pwd)/infra/terraform/.tflint.hcl" --chdir <root>
plugin "terraform" {
  enabled = true
  preset  = "recommended"
}
