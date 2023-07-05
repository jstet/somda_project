from somda_project.pipelines import pipe_wiki_views
import modal

image = modal.Image.debian_slim().poetry_install_from_file("pyproject.toml")

stub = modal.Stub(name="somda_project")


@stub.function(image=image, timeout=1000)
def f():
    pipe_wiki_views()


@stub.local_entrypoint()
def main():
    f.call()
