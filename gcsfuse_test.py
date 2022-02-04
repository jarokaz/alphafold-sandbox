from kfp import dsl
from kubernetes.client.models import V1EnvVar, V1SecretKeySelector

@dsl.pipeline(
    name='foo',
    description='hello world')
def foo_pipeline(tag: str, pull_image_policy: str):
  # any attributes can be parameterized (both serialized string or actual PipelineParam)
  op = dsl.ContainerOp(name='foo',
                      image='busybox:%s' % tag,
                      # pass in init_container list
                      init_containers=[dsl.UserContainer('print', 'busybox:latest', command='echo "hello"')],
                      # pass in sidecars list
                      sidecars=[dsl.Sidecar('print', 'busybox:latest', command='echo "hello"')],
                      # pass in k8s container kwargs
                      container_kwargs={'env': [V1EnvVar('foo', 'bar')]},
  )
  # set `imagePullPolicy` property for `container` with `PipelineParam`
  op.container.set_image_pull_policy(pull_image_policy)
  # add sidecar with parameterized image tag
  # sidecar follows the argo sidecar swagger spec
  op.add_sidecar(dsl.Sidecar('redis', 'redis:%s' % tag).set_image_pull_policy('Always'))