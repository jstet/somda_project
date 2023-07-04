from somda_project.helpers import download_file, gen_urls_old_wiki


def pipe_wiki_views():
    urls = gen_urls_old_wiki()
    filepath = download_file(urls[0])
    print(filepath)
