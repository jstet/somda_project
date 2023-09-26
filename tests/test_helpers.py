# from somda_project.helpers import compr_to_parquet, gen_urls
# from somda_project.IO_handlers import download_file
# import importlib_resources
# import requests
# import os


# def test_download_file():
#     filepath, _id = download_file(
#         {
#             "url": "https://dumps.wikimedia.org/other/pageview_complete/2013/2013-01/pageviews-20130101-user.bz2",
#             "id": "20130101",
#         }
#     )
#     assert os.path.isfile(filepath)
#     os.remove(filepath)
#     filepath, _id = download_file(
#         {
#             "url": "https://dumps.wikimedia.org/other/pagecounts-raw/2009/2009-05/pagecounts-20090521-010000.gz",
#             "id": "20090521-010000",
#         },
#         "gz",
#     )
#     assert os.path.isfile(filepath)
#     os.remove(filepath)


# # def test_gen_urls():
# #     urls = gen_urls()
# #     for url in urls:
# #         print(url)
# #         response = requests.head(url["url"])
# #         assert response.status_code == 200


# # format changes in 2011 and 2015
# def test_to_parquet_2009():
#     resource_path = importlib_resources.files("tests.data.pageviews_raw").joinpath("pagecounts-20090521-140000.gz")
#     path = str(resource_path)  # Convert the Path object to a string
#     output_path = compr_to_parquet(path, "20090521-140000", 2009)
#     assert os.path.isfile(output_path)
#     os.remove(output_path)
#     resource_path = importlib_resources.files("tests.data.pageviews_raw").joinpath("pagecounts-20090521-000000.gz")
#     path = str(resource_path)  # Convert the Path object to a string
#     output_path = compr_to_parquet(path, "20090521-000000", 2009)
#     assert os.path.isfile(output_path)
#     os.remove(output_path)


# #
# # def test_to_parquet_old():
# #     resource_path = importlib_resources.files("tests.data.pageviews_raw").joinpath("pageviews.bz2")
# #     path = str(resource_path)  # Convert the Path object to a string
# #     print(path)
# #     output_path = compr_to_parquet(path, "20130101", 2014)
# #     assert os.path.isfile(output_path)
# #     os.remove(output_path)
# #
# #
# # def test_to_parquet_new():
# #     resource_path = importlib_resources.files("tests.data.pageviews_raw").joinpath("pageviews-new.bz2")
# #     path = str(resource_path)  # Convert the Path object to a string
# #     print(path)
# #     output_path = compr_to_parquet(path, "20130101", 2019)
# #     assert os.path.isfile(output_path)
# #     os.remove(output_path)
