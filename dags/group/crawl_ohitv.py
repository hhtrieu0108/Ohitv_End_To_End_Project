from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

import requests
from bs4 import BeautifulSoup

def get_soup(url: str) -> BeautifulSoup:
    """
    Fetches and parses HTML content from a given URL.

    Args:
        url (str): The URL to retrieve and parse.

    Returns:
        BeautifulSoup: A BeautifulSoup object containing the parsed HTML.
    """

    html = requests.get(url)
    soup = BeautifulSoup(html.text,'html.parser')
    return soup

def get_url(ti) -> None:
    """
    Retrieves URLs for film categories from the main page and pushes them to XCom.

    Args:
        ti: Task instance to push data to XCom.

    Returns:
        None
    """

    url = "https://ohitv.cc/"
    soup = get_soup(url)
    kind_menu = soup.find_all('ul',class_='sub-menu')[1].find_all('a',href=True)
    kind_link = [link for link in kind_menu]
    kind_href = [link['href'] for link in kind_link]
    ti.xcom_push(key='get_url',value=kind_href)

def get_page(kind: str, soup: BeautifulSoup) -> list:
    """
    Generates pagination URLs for a given film category.

    Args:
        kind (str): The category of films.
        soup (BeautifulSoup): Parsed HTML of the category page.

    Returns:
        list: A list of pagination URLs.
    """

    pages = []
    try:
        page_element = soup.find('div',class_='pagination').find('span').text
        number_of_page = page_element.split(' ')[3]
    except:
        number_of_page = 1
    for page_number in range(1, int(number_of_page) + 1):
        pages.append(f"https://ohitv.info/the-loai/{kind}/page/{page_number}")
    return pages


def crawl_id(kind_href: list) -> list:
    """
    Extracts film IDs from category pages.

    Args:
        kind_href (list): List of category URLs.

    Returns:
        list: A list of film IDs.
    """
    
    id = []
    for link in kind_href:
        print(link)
        soup = get_soup(link)
        try:   
            data_html = soup.find('div',class_='items normal').find_all('article')
        except:
            data_html = soup.find('div',class_='items full').find_all('article')
        print(f"first page films : {len(data_html)}")
        id_1 = [id_param['id'] for id_param in data_html]
        id.extend(id_1)
        kind = link.split('/')[4]
        pages = get_page(kind=kind,soup=soup)
        print(len(pages))
        for page in pages:
            soup = get_soup(page)
            try:   
                data_html = soup.find('div',class_='items normal').find_all('article')
            except:
                data_html = soup.find('div',class_='items full').find_all('article')
            print(f"next page film : {len(data_html)}")
            id_2 = [id_param['id'] for id_param in data_html]
            id.extend(id_2)
    return id

def crawl_title(kind_href: list) -> list:
    """
    Extracts film titles from category pages.

    Args:
        kind_href (list): List of category URLs.

    Returns:
        list: A list of film titles.
    """

    title = []
    for link in kind_href:
        soup = get_soup(link)
        try:   
            data_html = soup.find('div',class_='items normal').find_all('data')
        except:
            data_html = soup.find('div',class_='items full').find_all('data')
        title_1 = [title_param.find('h3').text for title_param in data_html]
        title.extend(title_1)
        kind = link.split('/')[4]
        pages = get_page(kind=kind,soup=soup)
        for page in pages:
            soup = get_soup(page)
            try:   
                data_html = soup.find('div',class_='items normal').find_all('data')
            except:
                data_html = soup.find('div',class_='items full').find_all('data')
            title_2 = [title_param.find('h3').text for title_param in data_html]
            title.extend(title_2)
    return title

def crawl_film_link(kind_href: list) -> list:
    """
    Extracts film links from category pages.

    Args:
        kind_href (list): List of category URLs.

    Returns:
        list: A list of film links.
    """

    film_link = []
    for link in kind_href:
        soup = get_soup(link)
        try:   
            data_html = soup.find('div',class_='items normal').find_all('data')
        except:
            data_html = soup.find('div',class_='items full').find_all('data')
        film_link_1 = [link_param.find('h3').find('a',href=True)['href'] for link_param in data_html]
        film_link.extend(film_link_1)
        kind = link.split('/')[4]
        pages = get_page(kind=kind,soup=soup)
        for page in pages:
            soup = get_soup(page)
            try:   
                data_html = soup.find('div',class_='items normal').find_all('data')
            except:
                data_html = soup.find('div',class_='items full').find_all('data')
            film_link_2 = [link_param.find('h3').find('a',href=True)['href'] for link_param in data_html]
            film_link.extend(film_link_2)
    return film_link

def crawl_date(kind_href: list) -> list:
    """
    Extracts film release dates from category pages.

    Args:
        kind_href (list): List of category URLs.

    Returns:
        list: A list of film release dates.
    """

    date = []
    for link in kind_href:
        soup = get_soup(link)
        try:   
            data_html = soup.find('div',class_='items normal').find_all('data')
        except:
            data_html = soup.find('div',class_='items full').find_all('data')
        date_1 = [date_param.find('span').text for date_param in data_html]
        date.extend(date_1)
        kind = link.split('/')[4]
        pages = get_page(kind=kind,soup=soup)
        for page in pages:
            soup = get_soup(page)
            try:   
                data_html = soup.find('div',class_='items normal').find_all('data')
            except:
                data_html = soup.find('div',class_='items full').find_all('data')
            date_2 = [date_param.find('span').text for date_param in data_html]
            date.extend(date_2)
    return date

def crawl_rating(kind_href: list) -> list:
    """
    Extracts film ratings from category pages.

    Args:
        kind_href (list): List of category URLs.

    Returns:
        list: A list of film ratings.
    """

    rating = []
    for link in kind_href:
        soup = get_soup(link)
        rating_data = soup.find_all('div',class_='rating')
        rating_1 = [rating_param.text for rating_param in rating_data]
        rating.extend(rating_1)
        kind = link.split('/')[4]
        pages = get_page(kind=kind,soup=soup)
        for page in pages:
            soup = get_soup(page)
            rating_data = soup.find_all('div',class_='rating')
            rating_2 = [rating_param.text for rating_param in rating_data]
            rating.extend(rating_2)
    return rating

def crawl_quality(kind_href: list) -> list:
    """
    Extracts film qualities from category pages.

    Args:
        kind_href (list): List of category URLs.

    Returns:
        list: A list of film qualities.
    """

    quality = []
    for link in kind_href:
        soup = get_soup(link)
        quality_data = soup.find_all('div',class_='mepo')
        quality_1 = [quality_param.text for quality_param in quality_data]
        quality.extend(quality_1)
        kind = link.split('/')[4]
        pages = get_page(kind=kind,soup=soup)
        for page in pages:
            soup = get_soup(page)
            quality_data = soup.find_all('div',class_='mepo')
            quality_2 = [quality_param.text for quality_param in quality_data]
            quality.extend(quality_2)
    return quality

def crawl_genre(kind_href: list) -> list:
    """
    Extracts film genres from category pages.

    Args:
        kind_href (list): List of category URLs.

    Returns:
        list: A list of film genres.
    """

    genre = []
    for link in kind_href:
        soup = get_soup(link)
        genre_data = soup.find_all('div',class_='mta')
        for genre_data_sub in genre_data:
            sub_type = [text.text for text in genre_data_sub]
            genre.append(sub_type)
        kind = link.split('/')[4]
        pages = get_page(kind=kind,soup=soup)
        for page in pages:
            soup = get_soup(page)
            genre_data = soup.find_all('div',class_='mta')
            for genre_data_sub in genre_data:
                sub_type = [text.text for text in genre_data_sub]
                genre.append(sub_type)
    return genre

def crawl_short_description(kind_href: list) -> list:
    """
    Extracts short descriptions of films from category pages.

    Args:
        kind_href (list): List of category URLs.

    Returns:
        list: A list of short film descriptions.
    """

    short_des = []
    for link in kind_href:
        soup = get_soup(link)
        short_des_data = soup.find_all('div',class_='texto')
        short_des_1 = [short.text for short in short_des_data]
        short_des.extend(short_des_1)
        kind = link.split('/')[4]
        pages = get_page(kind=kind,soup=soup)
        for page in pages:
            soup = get_soup(page)
            short_des_data = soup.find_all('div',class_='texto')
            short_des_2 = [short.text for short in short_des_data]
            short_des.extend(short_des_2)
    return short_des

def crawl_all(ti) -> None:
    kind_href = ti.xcom_pull(key='get_url',task_ids='crawling.get_url')
    id = crawl_id(kind_href=kind_href)
    title = crawl_title(kind_href=kind_href)
    film_link = crawl_film_link(kind_href=kind_href)
    date = crawl_date(kind_href=kind_href)
    rating = crawl_rating(kind_href=kind_href)
    quality = crawl_quality(kind_href=kind_href)
    genre = crawl_genre(kind_href=kind_href)
    short_des = crawl_short_description(kind_href=kind_href)
    print("Going to push xcom...")
    ti.xcom_push(key='crawl',value=list(zip(id, title, film_link, date, rating, quality, genre, short_des)))

def crawl_tasks():
    with TaskGroup(
            group_id="crawling",
            tooltip="Crawling Ohitv"
    ) as group:

        get_url_task = PythonOperator(
            task_id='get_url',
            python_callable=get_url
        )

        crawl_all_task = PythonOperator(
            task_id='crawl_all',
            python_callable=crawl_all
        )

        get_url_task >> crawl_all_task

        return group