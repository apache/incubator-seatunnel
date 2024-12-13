---
sidebar_position: 4
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 使用Helm部署

使用Helm快速部署Seatunnel集群。

## 准备

我们假设您的本地已经安装如下软件:

- [docker](https://docs.docker.com/)
- [kubernetes](https://kubernetes.io/)
- [helm](https://helm.sh/docs/intro/quickstart/)

在您的本地环境中能够正常执行`kubectl`和`helm`命令。
 
以 [minikube](https://minikube.sigs.k8s.io/docs/start/) 为例, 您可以使用如下命令启动一个集群:

```bash
minikube start --kubernetes-version=v1.23.3
```

## 安装

使用默认配置安装
```bash
# Choose the corresponding version yourself
export VERSION=2.3.9
helm pull oci://registry-1.docker.io/apache/seatunnel-helm --version ${VERSION}
tar -xvf seatunnel-helm-${VERSION}.tgz
cd seatunnel-helm
helm install seatunnel .
```

如果您需要使用其他命名空间进行安装。
首先，需要更新 `value.yaml`
```
## 修改
seatunnel.default.svc.cluster.local
## 为
seatunnel.<your namespace >.svc.cluster.local

helm install seatunnel . -n <your namespace>
```

## 下一步
到现在为止，您已经安装好Seatunnel集群了，你可以查看Seatunnel有哪些[连接器](../../connector-v2).
或者选择其他方式 [部署](../../seatunnel-engine/deployment.md).
