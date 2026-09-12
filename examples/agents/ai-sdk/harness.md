# Testing from your laptop against the AWS harness

If you deployed `harness/terraform/aws` and want to run this example from your laptop: the cluster is VPC-only, so tunnel to the ALB through a node task with SSM, then point the example at the node hostnames.

Once:

```bash
brew install --cask session-manager-plugin
sudo sh -c 'echo "127.0.0.1 pico-1.picomq.internal pico-2.picomq.internal" >> /etc/hosts'
```

Terminal 1, leave it open (it prints `Waiting for connections...` and stays there):

```bash
export AWS_PROFILE=picomq-support AWS_REGION=us-east-1
ALB=$(cd harness/terraform/aws && terraform output -raw alb_dns_name)
TASK=$(aws ecs list-tasks --cluster picomq --service-name picomq-1 --query 'taskArns[0]' --output text)
RUNTIME=$(aws ecs describe-tasks --cluster picomq --tasks $TASK --query 'tasks[0].containers[0].runtimeId' --output text)

sudo -E aws ssm start-session --target "ecs:picomq_${TASK##*/}_${RUNTIME}" \
  --document-name AWS-StartPortForwardingSessionToRemoteHost \
  --parameters "{\"host\":[\"$ALB\"],\"portNumber\":[\"80\"],\"localPortNumber\":[\"80\"]}"
```

Terminal 2:

```bash
export AWS_PROFILE=picomq-support AWS_REGION=us-east-1
export PICO_TOKEN=$(aws secretsmanager get-secret-value \
  --secret-id "$(cd harness/terraform/aws && terraform output -raw bootstrap_secret_arn)" \
  --query SecretString --output text)
export PICO_ENDPOINT=http://pico-1.picomq.internal
export OPENAI_API_KEY=...

cd examples/agents/ai-sdk && npm run dev
```

Open http://localhost:3456
