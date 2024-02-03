from corpus import Corpus
from bs4 import BeautifulSoup
from pathlib import Path
if __name__ == "__main__":
    corpus_dir = r"C:\Users\dnp2k\Downloads\web_dump_zipfile\spacetime_crawler_data"
    c = Corpus(corpus_dir)
    url_data = c.fetch_url(input("Enter url:"))
    print(url_data)
    text = (BeautifulSoup(url_data["content"]).prettify())
    Path.touch("html.html")
    with open("html.html", 'w', encoding="utf-8") as html_file:
        html_file.write(text)