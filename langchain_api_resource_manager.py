import hashlib
import sqlite3
import requests
from bs4 import BeautifulSoup
from langchain.chat_models import ChatOllama
from readability import Document
import multiprocessing

langchain_api_refer_url = "https://python.langchain.com/api_reference/index.html"
langchain_api_refer_url_base = "https://python.langchain.com/api_reference/"
unuseful_keywords = ["This module", "This class", "This function", "class", "function", "method", "property", "Base", "Abstract", "Interface", "required", 'str', 'dict', 'list', 'any', 'optional']

update_cnt = 0
conn = sqlite3.connect('langchain_api_resource.db', timeout=5)
cursor = conn.cursor()
cursor.execute('SELECT MAX(LENGTH(description)) FROM langchain_api_resource')
maxlen = cursor.fetchone()[0]
conn.close()

def create_table(conn):
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS langchain_api_resource (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT NOT NULL,
            description TEXT,
            checksum INTEGER NOT NULL,
            keywords TEXT,
            type TEXT,
            depth INTEGER,
            parent_id INTEGER,
            children_ids TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()

def get_item_from_url(url, items:[str]):
    conn = sqlite3.connect('langchain_api_resource.db', timeout=5)
    cursor = conn.cursor()
    query = 'SELECT ' + ', '.join(items) + ' FROM langchain_api_resource WHERE url = ?'
    cursor.execute(query, (url,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result

def add_item(url, checksum, type, depth, parent_id):
    conn = sqlite3.connect('langchain_api_resource.db', timeout=5)
    cursor = conn.cursor()
    
    # exception logic 
    cursor.execute('SELECT id FROM langchain_api_resource WHERE url = ?', (url,))
    existing_item = cursor.fetchone()
    if existing_item:
        return existing_item[0]
    
    # insert logic
    cursor.execute('''
        INSERT INTO langchain_api_resource (url, checksum, type, depth, parent_id)
        VALUES (?, ?, ?, ?, ?)
    ''', (url, checksum, type, depth, parent_id))
    conn.commit()
    return cursor.lastrowid

def update_item(update_flag, id, description=None, keywords=None, children_ids=None):
    global update_cnt
    update_cnt += 1
    print_update_cnt(id)
    if update_flag == False:
        return
    conn = sqlite3.connect('langchain_api_resource.db', timeout=5)
    cursor = conn.cursor()
    update_query = "UPDATE langchain_api_resource SET updated_at = CURRENT_TIMESTAMP"
    update_params = []

    if description is not None:
        update_query += ", description = ?"
        update_params.append(description)
    
    if keywords is not None:
        keywords_str = ','.join(keywords)
        update_query += ", keywords = ?"
        update_params.append(keywords_str)
    
    if children_ids is not None:
        children_ids_str = ','.join(map(str, children_ids))
        update_query += ", children_ids = ?"
        update_params.append(children_ids_str)
    
    update_query += " WHERE id = ?"
    update_params.append(id)

    cursor.execute(update_query, update_params)
    conn.commit()

def get_checksum(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Remove header and footer
    for tag in soup(['header', 'footer']):
        tag.decompose()
    
    # Extract main content
    content = soup.get_text()
    
    # Calculate checksum
    checksum = hashlib.md5(content.encode('utf-8')).hexdigest()
    
    return checksum


def generate_langchain_api_resource_db():
    conn = sqlite3.connect('langchain_api_resource.db', timeout=5)
    create_table(conn)
    print("langchain_api_resource 테이블이 성공적으로 생성되었습니다.")

def get_category_hrefs(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    nav_element = soup.find('div', class_='bd-toc-item navbar-nav')
    if nav_element:
        links = nav_element.find_all('a', class_='reference internal')
        hrefs = [link.get('href') for link in links if link.get('href')]
        return hrefs
    return []

def parse_page_get_internal_category_hrefs(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    article_section = soup.find('article', class_='bd-article')
    if article_section:
        links = article_section.find_all('a', class_='reference internal')
        hrefs = []
        for link in links:
            if link.find('span', class_='std std-ref'):
                hrefs.append(link.get('href'))
        return hrefs
    return []

def page_parse_get_classes(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    classes_section = soup.find('p', string='Classes')
    if classes_section:
        table_container = classes_section.find_next('div', class_='pst-scrollable-table-container')
        if table_container:
            links = table_container.find_all('a', class_='reference internal')
            hrefs = [link.get('href') for link in links if link.get('href')]
            return hrefs
    return []

def page_parse_get_functions(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    classes_section = soup.find('p', string='Functions')
    if classes_section:
        table_container = classes_section.find_next('div', class_='pst-scrollable-table-container')
        if table_container:
            links = table_container.find_all('a', class_='reference internal')
            hrefs = [link.get('href') for link in links if link.get('href')]
            return hrefs
    return []

def extract_description(content):
    llm = ChatOllama(
        model="mistral",
        temperature=0.1,
        device = "device",
        # 생성 부분 추가
        generation_kwargs={
            "max_tokens": 4096,                 #max_tokens: 생성할 최대 토큰 수를 4096으로 설정합니다.
            "top_p": 0.95,                      #top_p: 누적 확률 분포에서 상위 95%의 토큰만 고려합니다.
            "top_k": 50,                        #top_k: 각 단계에서 가장 가능성 있는 50개의 토큰만 고려합니다.
            "repeat_penalty": 1.1,              #repeat_penalty: 반복을 피하기 위해 이미 생성된 토큰에 1.1의 페널티를 적용합니다.
            "stop": ["[INST]", "[/INST]"],      #stop: "[INST]"와 "[/INST]"를 만나면 생성을 중지합니다.
        }
    )

    prompt = f"[INST]This is a api reference content. Provide a brief description of the following content UNDER 35 words ,concisely:\n\n{content[:1000]}...[/INST]"   # need custom : content
    return llm.invoke(prompt).content
    

def extract_keywords(content):
    llm = ChatOllama(
        model="mistral",
        temperature=0.1,
        device = "device",
        # 생성 부분 추가
        generation_kwargs={
            "max_tokens": 4096,                 #max_tokens: 생성할 최대 토큰 수를 4096으로 설정합니다.
            "top_p": 0.95,                      #top_p: 누적 확률 분포에서 상위 95%의 토큰만 고려합니다.
            "top_k": 50,                        #top_k: 각 단계에서 가장 가능성 있는 50개의 토큰만 고려합니다.
            "repeat_penalty": 1.1,              #repeat_penalty: 반복을 피하기 위해 이미 생성된 토큰에 1.1의 페널티를 적용합니다.
            "stop": ["[INST]", "[/INST]"],      #stop: "[INST]"와 "[/INST]"를 만나면 생성을 중지합니다.
        }
    )
    prompt = f"""[INST]
    You are a professional programmer's assistant. Please extract keywords based on the given content.
    FOLLOW THIS FORMAT : keyword1, keyword2, keyword3
    
    This is a api reference content. Provide a list of keywords. It will be used for search and filter function, so need to be more granular and specific.:
    {content[:4000]}
    [/INST]"""   # need custom : content
    return llm.invoke(prompt).content

def extract_keywords_and_description(content):
    description = extract_description(content)
    keywords = extract_keywords(content)
    return keywords, description

def refine_keywords(keywords):
    keywords = keywords.split(',')
    # 키워드에서 따옴표 제거
    keywords = [keyword.strip().replace("'", "").replace('"', '') for keyword in keywords]
    # 중복 제거
    keywords = list(set(keywords))
    # unuseful_keywords 제거
    keywords = [keyword for keyword in keywords if keyword not in unuseful_keywords]
    return keywords

def get_description_and_keywords(id):
    update_flag = True
    conn = sqlite3.connect('langchain_api_resource.db', timeout=5)
    cursor = conn.cursor()
    cursor.execute("SELECT url, checksum, description, keywords FROM langchain_api_resource WHERE id = ?", (id,))
    result = cursor.fetchone()
    if result:
        url, checksum, description, keywords = result
    else:
        url, checksum, description, keywords = None, None, None, None
    # Calculate checksum
    
    response = requests.get(url)
    response.raise_for_status()
    # new_checksum = get_checksum(url)
    # if new_checksum == checksum:
    #     if description and keywords:
    #         update_flag = False
    #         refined_keywords = keywords.split(',') if keywords else []
    #         cursor.close()
    #         return update_flag, refined_keywords, description
    if description and keywords:
        update_flag = False
        refined_keywords = keywords.split(',') if keywords else []
        cursor.close()
        return update_flag, refined_keywords, description
    
    soup = BeautifulSoup(response.text, 'html.parser')
    bd_article = soup.find('article', class_='bd-article')    
    # Use the readability library to extract the main body
    # Now parse the main body using BeautifulSoup
    # soup = BeautifulSoup(bd_article, 'html.parser')
    content = bd_article.get_text(separator='\n', strip=True)
    keywords, description = extract_keywords_and_description(content)
    refined_keywords = refine_keywords(keywords)
    cursor.close()
    return update_flag, refined_keywords, description

def integrate_descriptions(id, descriptions:list[str]) -> str:
    update_flag = True
    conn = sqlite3.connect('langchain_api_resource.db', timeout=5)
    cursor = conn.cursor()
    cursor.execute("SELECT url, checksum, description FROM langchain_api_resource WHERE id = ?", (id,))
    result = cursor.fetchone()
    if result:
        url, checksum, description = result
    else:
        url, checksum, description = None, None, None
    
    response = requests.get(url)
    response.raise_for_status()
    
    # new_checksum = get_checksum(url)
    # if new_checksum == checksum and description is not None:
    #     update_flag = False
    #     return update_flag, description
    if description and len(description) > 0:
        update_flag = False
        return update_flag, description
    
    content = response.text
    description = extract_description(content)
    cursor.close()
    return update_flag, description

def integrate_keywords(id, keywords:list[list[str]]) -> list[str]:
    update_flag = True
    conn = sqlite3.connect('langchain_api_resource.db', timeout=5)
    cursor = conn.cursor()
    cursor.execute("SELECT keywords FROM langchain_api_resource WHERE id = ?", (id,))
    keywords_db = cursor.fetchone()[0]
    if keywords_db and len(keywords_db) > 0:
        update_flag = False
        return update_flag, keywords_db.split(',')
    
    flat_keywords = [item for sublist in keywords for item in sublist]
    unique_keywords = list(set(flat_keywords))
    return update_flag, unique_keywords

def page_parse_add_update_loop(parent_url, parent_depth, parent_id):
    base_url = parent_url.rsplit('/', 1)[0] + "/"
    depth = parent_depth + 1
    internal_category_hrefs = parse_page_get_internal_category_hrefs(parent_url)
    internal_category_links = [base_url + href for href in internal_category_hrefs]
    children_ids = []
    children_descriptions = []
    children_keywords = []
    if len(internal_category_links) == 0:
        # find classes 
        classes = page_parse_get_classes(parent_url)
        class_links = [base_url + href for href in classes]
        for class_link in class_links:            
            # id = add_item(class_link, get_checksum(class_link), "class", depth, parent_id)
            id = get_item_from_url(class_link, ["id"])[0]
            children_ids.append(id)
            # get description and keywords
            update_flag, keywords, description = get_description_and_keywords(id)
            children_descriptions.append(description)
            children_keywords.append(keywords)
            update_item(update_flag, id, description=description, keywords=keywords)
        # find functions 
        functions = page_parse_get_functions(parent_url)
        function_links = [base_url + href for href in functions]
        for function_link in function_links:            
            # id = add_item(function_link, get_checksum(function_link), "function", depth, parent_id)
            id = get_item_from_url(function_link, ["id"])[0]
            children_ids.append(id)
            # get description and keywords
            update_flag, keywords, description = get_description_and_keywords(id)
            children_descriptions.append(description)
            children_keywords.append(keywords)
            update_item(update_flag, id, description=description, keywords=keywords)
        pass 
    else:
        for internal_category_link in internal_category_links:
            # id = add_item(internal_category_link, get_checksum(internal_category_link), "category", depth, parent_id)            
            id = get_item_from_url(internal_category_link, ["id"])[0]
            page_parse_add_update_loop(internal_category_link, depth, id)
            keywords = get_item_from_url(internal_category_link, ["keywords"])[0]
            children_keywords.append(keywords.split(','))
            if parent_depth == 1:
                print(f"parent_id : {parent_id}\nintegrated_keywords : {children_keywords}")
    # finish loop    
    update_flag_description, integrated_description = integrate_descriptions(parent_id, children_descriptions)
    update_flag_keywords, integrated_keywords = integrate_keywords(parent_id, children_keywords)
    update_flag = update_flag_description or update_flag_keywords
    if parent_depth == 1:
        print(f"parent_id : {parent_id}\nintegrated_keywords : {integrated_keywords}")
    update_item(update_flag, parent_id, description=integrated_description, keywords=integrated_keywords, children_ids=children_ids)

def process_category(category_link):
    checksum = 0 #get_checksum(category_link)
    id = add_item(category_link, checksum, "category", 1, 0)
    page_parse_add_update_loop(category_link, 1, id)

def print_update_cnt(id):
    print(f"update id : {id}, update cnt : {update_cnt}")
    # global update_cnt
    # global maxlen
    # percent_1 = maxlen / 100
    # if update_cnt % percent_1 == 0:
    #     print("#", end="", flush=True)


if __name__ == '__main__':
    conn = sqlite3.connect('langchain_api_resource.db', timeout=5)
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM langchain_api_resource WHERE description IS NULL OR description = ""')
    not_filled_description_number = cursor.fetchone()[0]
    conn.close()
    print(f"Not filled description number: {not_filled_description_number}")
    
    generate_langchain_api_resource_db()

    category_hrefs = get_category_hrefs(langchain_api_refer_url)
    category_links = [langchain_api_refer_url_base + href for href in category_hrefs]    
    for link in category_links[-1:]:
        process_category(link)
    # pool = multiprocessing.Pool()
    # pool.starmap(process_category, [(link,) for link in category_links])
    # pool.close()
    # pool.join()