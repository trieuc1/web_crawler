import logging
import re
from urllib.parse import urlparse, urljoin
from lxml import html, etree
from pathlib import Path
import requests
logger = logging.getLogger(__name__)

class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    def __init__(self, frontier, corpus):
        self.frontier = frontier
        self.corpus = corpus

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        
        Path.touch("url_links.txt")
        with Path.open("ur_link.txt", "a") as txt:
            while self.frontier.has_next_url():
                url = self.frontier.get_next_url()
                logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))

                url_data = self.corpus.fetch_url(url)
                # Write links to txt
                
                txt.write(url + "\n")
                for next_link in self.extract_next_links(url_data):
                    if self.is_valid(next_link):
                        #if a link is valid, it will grab all the words in the file, returns it as a list
                        all_words_in_file = self.words_in_link(next_link)
                        #print(all_words_in_file)

                        if self.corpus.get_file_name(next_link) is not None:
                            self.frontier.add_url(next_link)

    def extract_next_links(self, url_data):
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. 
        This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.

        Suggested library: lxml
        """
        url = url_data.get('url')
        content = url_data.get('content')

        if url is not None and content is not None:
            # parses html content
            try:
                tree = html.fromstring(content, '')
                # gets all relative and absolute links
                all_links = tree.xpath("//a/@href")
                # turns every relative link into absolute
                absolute_urls =[urljoin(url, link) for link in all_links]
                return absolute_urls
            except (etree.ParserError):
                return []
        else:
            return []
        

    def words_in_link(self, url_content):
        """
        Function that gets the file content. It grabs the anything in the p, h1, h2, h3, h4, h5, h6, span and div tabs. (Tabs that generally include words)
        This function returns the list of words in that file.
        """
        response = requests.get(url_content)
        tree = html.fromstring(response.content)
        text_elements = tree.xpath('//p | //h1 | //h2 | //h3 | //h4 | //h5 | //h6 | //span | //div')
        words = []

        for element in text_elements:
            if element.text_content():
                list_of_words = re.split(r'[\s\t\r\n]+', element.text_content())
                for word in list_of_words:
                    if ((not word.isspace()) and (len(word) >= 1)):
                        words.append(word)
        
        return words
            

    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        try:
            if ( ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())):
                return True
            else:
                return False
            
        except TypeError:
            print("TypeError for ", parsed)
            return False
        

