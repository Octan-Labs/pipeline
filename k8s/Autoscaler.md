eksctl utils associate-iam-oidc-provider \
    --cluster sparkonk8s \
    --profile octan-mainaccount \
    --region ap-southeast-1 \
    --approve

aws iam create-policy   \
  --policy-name k8s-asg-policy \
  --policy-document file://k8s-asg-policy.json \
  --profile octan-mainaccount --region ap-southeast-1

eksctl create iamserviceaccount \
    --name cluster-autoscaler \
    --namespace kube-system \
    --cluster sparkonk8s \
    --attach-policy-arn "arn:aws:iam::171092530978:policy/k8s-asg-policy" \
    --approve \
    --override-existing-serviceaccounts \
    --profile octan-mainaccount --region ap-southeast-1

kubectl apply -f autoscaler.yaml
