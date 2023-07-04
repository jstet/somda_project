from somda_project.helpers import gen_urls_old_wiki, download_file
import modal

image = modal.Image.debian_slim().poetry_install_from_file("pyproject.toml")

stub = modal.Stub(name="somda_project")


@stub.function(image=image)
def f():
    print("Hu")
    urls = gen_urls_old_wiki()
    download_file(urls[0])


@stub.local_entrypoint()
def main():
    f.call()
