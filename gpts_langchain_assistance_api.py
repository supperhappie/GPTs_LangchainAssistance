from fastapi import FastAPI, Query
from pydantic import BaseModel, Field
from langchain.chat_models import ChatOllama
import sqlite3
from fuzzywuzzy import fuzz
# run cml : uvicorn samples_langchain.7_0_simple_fastapi:app --reload
# http://127.0.0.1:8000/docs
# http://127.0.0.1:8000/quote
# distribute : cloudflared tunnel --url http://127.0.0.1:8000
# note : install cml for cloudflared is "winget install --id Cloudflare.cloudflared" refer to homepage 

criteria_rate = 0.01

app = FastAPI(
    title="Langchain API Assistant",
    description="Langchain API Assistant. if you want to use langchain api, please refer to this api.",
)

def is_similar(str1, str2):
    threshold=40
    """
    두 문자열의 유사도를 계산하여 threshold 값 이상이면 True를 반환
    :param str1: 첫 번째 문자열
    :param str2: 두 번째 문자열
    :param threshold: 유사도 임계값 (0~100)
    :return: bool 값으로 유사 여부 반환
    """
    similarity_score = fuzz.ratio(str1, str2)
    return similarity_score >= threshold

class question_answer(BaseModel):
    answer: str = Field(
        description="The answer for the question.",
    )
    urls: str = Field(
        description="related url list for the question.",
    )


# app.add_api_route(
#     "/question",
#     question_answer(question= Query(..., description="The question to be answered")),
#     methods=["GET"],
#     summary="Returns the answer for the question refer to langchain api resource",
#     description="Upon receiving a GET request this endpoint will return a answer and related url list for the question.",
#     response_description="A question_answer object that contains the answer and related url list for the question.",
#     response_model=question_answer,
# )

def gen_keywords(question: str)->list[str]:
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
    You are a professional programmer's assistant. Please extract keywords based on the given content. at least 10 keywords.
    FOLLOW THIS FORMAT : keyword1, keyword2, keyword3
    This is a api reference content. Provide a list of keywords. It will be used for search and filter function, so need to be more granular and specific.:
    
    {question}. It is about langchain api.
    [/INST]"""    
    keywords = llm.invoke(prompt).content
    return keywords.split(',')
    
def search_target_urls(question: str, keywords: list[str], depth: int = 1)-> list[str]:        
    conn = sqlite3.connect('langchain_api_resource.db', timeout=5)
    cursor = conn.cursor()
    cursor.execute("SELECT id, keywords FROM langchain_api_resource WHERE depth = ?", (depth,))
    results = cursor.fetchall()
    conn.close()
    keywords_len = len(keywords)
    max_match_rate = criteria_rate
    bucket_id = []
    bucket_url = []
    for id, keywords_db in results:
        if keywords_db is '' or keywords_db is None:
            continue
        match_count = sum(1 for keyword in keywords if is_similar(keyword, keywords_db))
        match_rate = match_count / keywords_len
        if match_rate > max_match_rate:
            max_match_rate = match_rate
            bucket_id.clear
            bucket_id.append(id)
        elif match_rate == max_match_rate:
            bucket_id.append(id)
    # if child_id none, finish search
    for id in bucket_id:
        conn = sqlite3.connect('langchain_api_resource.db', timeout=5)
        cursor = conn.cursor()
        cursor.execute("SELECT url, children_ids FROM langchain_api_resource WHERE id = ?", (id,))
        url, children_ids = cursor.fetchone()
        conn.close()
        bucket_url.append(url)
        if children_ids is not None:
            urls = search_target_urls(question, keywords, depth+1)
            bucket_url.append(urls)

    return bucket_url

@app.get("/question_answer")
async def question_answer(question: str = Query(..., description="The question to be answered")):
    keywords = gen_keywords(question)
    urls = search_target_urls(question, keywords)
    print(urls)
    return {
        "answer": f"{question} is answered",
        "urls": "1. https://www.google.com, 2. https://www.naver.com",
    }
    # return question_answer(question)

# test
question = "UsageMetadata sample code"
keywords = gen_keywords(question)
print(keywords)
urls = search_target_urls(question, keywords, 2)
print(urls)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000, reload=True)