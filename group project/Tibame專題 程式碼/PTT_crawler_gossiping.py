import os
import datetime
import time
import random
import re
import pandas as pd
import requests as rq
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

# 爬蟲目標看板名稱(Ptt批踢踢)
boardName = 'Gossiping'

# 儲存tsv資料夾名稱
saveFileFolder = 'ptt_' + boardName


# 設定header與cookie
my_headers = {'cookie': 'over18=1;', # Ptt網站 18歲的認證
              'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.128 Safari/537.36'}

# 設定起始頁數
startPage = 38852

# 設定結束頁數
endPage = 39913

# url = 'https://www.ptt.cc/bbs/Gossiping/index.html'
# res = rq.get(url, headers=my_headers)
# soup = BeautifulSoup(res.text, 'html.parser')
# endPage = int(re.findall('index(\d+).html', soup.select('a.btn.wide')[1].get('href'))[0])

# 目前爬蟲的進度
# latestPageNumsList = [re.findall(r'(\d+).tsv', elem)[0] for elem in os.listdir('./ptt_Gossiping') if re.findall(r'(\d+).tsv', elem)]
# if latestPageNumsList:
#     # 判斷目前儲存到第幾頁
#     latestPageNums = max(latestPageNumsList)
#     # 調整起始頁數
#     startPage = int(latestPageNums) + 1


# 建立 MySQL(資料庫)連線引擎
engine = create_engine('mysql+mysqlconnector://ceb102:gl3y3sm3cj84@35.229.251.211:3306/project')

# 建立 Pandas DataFrame (先建立欄位 並排序欄位順序)
df1 = pd.DataFrame(columns=["url_id", "ptt_board", "authors", "datetime_list", "websites"])
df2 = pd.DataFrame(columns=["ptt_title", "pushtext_count"])
df3 = pd.DataFrame(columns=["article_content"])
df4 = pd.DataFrame(columns=["url_id", "tags", "messages_id", "messages", "messages_time"])




# 爬取 Ptt 第一層的資訊
for page_number in range(startPage, endPage+1):
    print('目前正在爬取第 ', page_number, '個頁面 進度: ', page_number, ' / ', endPage)
    url1 = 'https://www.ptt.cc/bbs/' + boardName + '/index%s.html' % (page_number)
    res1 = rq.get(url1, headers=my_headers) # 使用requests的get方法把網頁內容載下來(第一層)


    # 轉為soup格式
    soup1 = BeautifulSoup(res1.text, 'html.parser')  # 使用 html.parser 作為解析器

    # -----------------------------取得Ptt第一層基本資訊----------------------------- #
    # 使用find_all()找出所有<div class="r-ent">區塊  並逐一訪問  取得資料
    r_ents = soup1.find_all("div", "r-ent")
    for r in r_ents:
        titles = r.find("div", "title")                    # 取得 Ptt標題 資訊
        # dates = r.find("div", "meta").find("div", "date")  # 取得 日期 資訊
        counts = r.find("div", "nrec")                     # 取得 推文數 資訊



        s2 = pd.Series([titles.text, counts.text],
                       index=["ptt_title", "pushtext_count"])
        df2 = df2.append(s2, ignore_index=True) # df2 DataFrame中添加s2的數據  


############################################################       
# 刪除文章標題內有'刪除'字眼的資料
        df2 = df2[ ~ df2['ptt_title'].str.contains('刪除') ]
##############################################################



    time.sleep(random.randint(0, 1))  # 怕資料庫會ban掉 因此休息0-1秒之間
    # -----------------------------取得Ptt第一層基本資訊----------------------------- #

    # 爬取 Ptt 第二層的資訊
    all_titles = soup1.select("div.title") # 爬取該頁所有的標題

########################################
# 如果文章標題內有刪除 則不進迴圈 不取資料
    n = '刪除'
    for item in all_titles:
        if n in str(item):
            # print('123')
            continue
        else:
            a_item = item.select_one("a") # 爬取到該頁的所有連結
######################################
            
            # if a_item  (是因為可能會有文章刪除或不存在的可能性 會得到None)
            if a_item:
                url2 = 'https://www.ptt.cc' + a_item.get('href')  # url2 用來爬取每一頁的所有文章連結
                print(f"正在處理的網址：{url2}")
                res2 = rq.get(url2, headers=my_headers) # 使用requests的get方法把網頁內容載下來(第二層)



                # 轉為soup格式
                soup2 = BeautifulSoup(res2.text, 'html.parser') # 使用 html.parser 作為解析器

                # 取得 文章ID 資訊 (使用正規表達式 找出規則 並爬取到「文章ID」資訊)
                url_id = re.findall(r'(\w+\.\w+\.\w+\.\w+).html', url2)[0]

                # -------------------------------------取得文章基本資訊------------------------------------- #
                main_content = soup2.select("#main-content")
                for m in main_content:
                    infosTag = m.find_all("span", class_="article-meta-tag")
                    infos = m.find_all("span", class_="article-meta-value")

                    # 例外處理 (特殊情況 因為其中有幾篇文章 沒有作者...等資訊)
                    matchSite = [i for i, e in enumerate(infosTag) if e.text == '作者']
                    authors = infos[matchSite[0]].text if matchSite else None           # 取得 文章作者 資訊
                    matchSite = [i for i, e in enumerate(infosTag) if e.text == '看板']
                    ptt_board = infos[matchSite[0]].text if matchSite else None         # 取得 看板名稱 資訊
                    matchSite = [i for i, e in enumerate(infosTag) if e.text == '時間']
                    time_list = infos[matchSite[0]].text if matchSite else None         # 取得 文章時間 資訊

                    try:
                        if time_list:
                            time_list = datetime.datetime.strptime(time_list, '%a %b %d %H:%M:%S %Y')
                    except Exception as e:
                        time_list = None

                    s1 = pd.Series([url_id, authors, ptt_board, time_list, url2],
                                index=["url_id", "authors", "ptt_board", "datetime_list", "websites"])
                    df1 = df1.append(s1, ignore_index=True) # df1 DataFrame中添加s1的數據
                time.sleep(random.randint(0, 1)) # 休息0-1秒之間
                # -------------------------------------取得文章基本資訊------------------------------------- #

                # -----------------------------------------取得文章內容----------------------------------------- #
                # 先使用find() 是因為網頁中所得到的資料為一區塊
                contents = soup2.find("div", id="main-content")

                # 單用find_all()很難找到目標  因為文章內容and基本資訊...等都是混在一起  因此使用extract()把不要的東西去掉
                msg1 = contents.find_all("div", class_="article-metaline")
                for s in msg1:
                    s.extract()   # 去掉  作者 標題 時間

                msg2 = contents.find_all("div", class_="article-metaline-right")
                for s in msg2:
                    s.extract()   # 去掉  看板

                # 取得文章內容
                s3 = pd.Series([contents.text.split('--')[0]],  # split('--')[0] 去掉留言
                            index=["article_content"])
                df3 = df3.append(s3, ignore_index=True) # df3 DataFrame中添加s3的數據
                time.sleep(random.randint(0, 1)) # 休息0-1秒之間
                # -----------------------------------------取得文章內容----------------------------------------- #

                # ---------------------------------------取得留言內容--------------------------------------- #
                for f in soup2.select("div.push"):

                    if 'center' not in f['class']:
                        tags = f.find("span", "push-tag").getText() # 取得 推.噓.→ 標籤內容
                        messages_id = f.find("span", "push-userid").getText()
                        messages = f.find("span", "push-content").getText().replace(':', '').strip()  # 去掉冒號及左右的空白
                        messages_time = f.find("span", "push-ipdatetime").getText().strip()           # 去掉左右的空白

                        s4 = pd.Series([url_id, tags, messages_id, messages, messages_time],
                                    index=["url_id", "tags", "messages_id", "messages", "messages_time"])
                        df4 = df4.append(s4, ignore_index=True) # df4 DataFrame中添加s4的數據
                time.sleep(random.randint(0, 1)) # 休息0-1秒之間
                # ---------------------------------------取得留言內容--------------------------------------- #

        # 每跑5頁時儲存資料
    if (page_number % 1 == 0) | (page_number == endPage+1):

        print('目前正在儲存 ', str(page_number), ' .tsv資料')

            # df1.df2.df3 DataFrame合併  並匯出成TSV檔
        dfs = [df1, df2, df3]
        mainTextDf = pd.concat(dfs, axis=1).reset_index(drop=True)
        mainTextDf.to_csv("./" + saveFileFolder + "/ptt_gossiping_maintext_" + str(page_number) + ".tsv",
                                encoding="utf-8-sig", index=False, sep='\t')

            # df4 DataFrame 匯出成TSV檔
        df4.to_csv("./" + saveFileFolder + "/ptt_gossiping_comments_" + str(page_number) + ".tsv",
        encoding="utf-8-sig", index=False, sep='\t')

            # 匯入 MySQL 資料庫 (DataFrame to MySQL)
        c = engine.connect()  # 準備連線
        mainTextDf.to_sql(name='ptt_gossiping_maintext', con=c, index=False, if_exists='append')
        df4.to_sql(name='ptt_gossiping_comments', con=c, index=False, if_exists='append')
        c.close()  # 關閉連線
        engine.dispose()  # 關閉 MySQL(資料庫)連線引擎

            # 清空資料
        df1 = pd.DataFrame(columns=["url_id", "ptt_board", "authors", "datetime_list", "websites"])
        df2 = pd.DataFrame(columns=["ptt_title", "pushtext_count"])
        df3 = pd.DataFrame(columns=["article_content"])
        df4 = pd.DataFrame(columns=["url_id", "tags", "messages_id", "messages", "messages_time"])