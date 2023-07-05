from somda_project.helpers import download_file, gen_urls_old_wiki, bz2_to_parquet
import os


def pipe_wiki_views():
    urls = gen_urls_old_wiki()
    bz2_path, id_ = download_file(urls[0])
    bz2_to_parquet(bz2_path, id_)
    os.remove(bz2_path)
