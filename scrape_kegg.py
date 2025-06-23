import requests
from bs4 import BeautifulSoup, NavigableString
import pandas as pd
import time
from tqdm import tqdm
import re

# --- 配置 ---
BASE_URL = "https://www.kegg.jp"
START_URL = "https://www.kegg.jp/kegg-bin/show_organism?menu_type=category_info&category=Plants"
OUTPUT_FILE = "kegg_plant_pathways_final_robust.csv"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
}
# --- 健壮性配置 ---
RETRY_COUNT = 3  # 单个URL请求失败后的重试次数
RETRY_DELAY = 5  # 重试前的等待时间（秒）
REQUEST_DELAY = 1.5 # 每个物种请求之间的常规等待时间（秒），从0.5增加

def get_organism_list(url):
    """
    第一步：获取物种列表（此函数已验证可用，无需修改）。
    """
    print("Step 1: Fetching organism list using table structure...")
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        organisms = []
        table_rows = soup.find_all('tr')
        if not table_rows:
            print("Error: Could not find any table rows (<tr>).")
            return []
        for row in table_rows:
            cells = row.find_all('td')
            if len(cells) >= 3:
                org_code = cells[1].get_text(strip=True)
                latin_name = cells[2].get_text(strip=True)
                organisms.append({'code': org_code, 'name': latin_name})
        print(f"Found {len(organisms)} organisms.")
        return organisms
    except requests.exceptions.RequestException as e:
        print(f"Error fetching organism list: {e}")
        return []

def get_pathway_data_for_organism(org_code, latin_name):
    """
    第三步：为单个物种获取其所有的 Pathway 数据。
    *** 已完全重写以适应新的 <b> 和 <ul> 结构，并增加了重试逻辑 ***
    """
    pathway_url = f"{BASE_URL}/kegg-bin/show_organism?menu_type=pathway_maps&org={org_code}"
    
    # --- 增加重试逻辑 ---
    for attempt in range(RETRY_COUNT):
        try:
            response = requests.get(pathway_url, headers=HEADERS, timeout=20) # 增加超时
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            pathways = []
            
            # --- 全新解析逻辑开始 ---
            # 查找所有作为 Level 1 分类的 <b> 标签
            l1_tags = soup.find_all('b')
            
            for l1_tag in l1_tags:
                current_l1 = l1_tag.get_text(strip=True)
                
                # Level 2 和 Level 3 的信息在紧跟在 <b> 后面的 <ul> 标签里
                outer_ul = l1_tag.find_next_sibling('ul')
                if not outer_ul:
                    continue

                current_l2 = ""
                # 遍历 outer_ul 的直接子元素
                for element in outer_ul.children:
                    # Level 2 是没有被标签包裹的文本节点
                    if isinstance(element, NavigableString) and element.strip():
                        current_l2 = element.strip()
                    
                    # Level 3 的条目在内层的 <ul> 标签里
                    elif element.name == 'ul':
                        # 查找所有 pathway 的链接 <a>
                        for a_tag in element.find_all('a'):
                            pathway_name = a_tag.get_text(strip=True)
                            pathway_link = BASE_URL + a_tag['href']
                            
                            # KEGG ID 在 <a> 标签之前的文本节点里
                            id_text_node = a_tag.previous_sibling
                            if id_text_node and isinstance(id_text_node, NavigableString):
                                kegg_id = id_text_node.strip()
                            else:
                                kegg_id = 'N/A' # Failsafe

                            pathways.append({
                                'Organism Latin Name': latin_name,
                                'Level 1 Category': current_l1,
                                'Level 2 Category': current_l2,
                                'KEGG ID': "ko" + kegg_id,
                                'Pathway Name': pathway_name,
                                'URL': pathway_link
                            })
            # --- 全新解析逻辑结束 ---
            
            return pathways # 如果成功，返回数据并退出函数

        except requests.exceptions.RequestException as e:
            # 如果请求失败（包括连接重置），打印错误并准备重试
            tqdm.write(f"Error for {org_code} (attempt {attempt+1}/{RETRY_COUNT}): {e}. Retrying in {RETRY_DELAY}s...")
            if attempt < RETRY_COUNT - 1:
                time.sleep(RETRY_DELAY)
            else:
                tqdm.write(f"Failed to fetch data for {org_code} after {RETRY_COUNT} attempts.")

    return [] # 如果所有重试都失败，返回空列表

def main():
    """
    主函数，协调整个爬取过程。
    """
    organisms = get_organism_list(START_URL)
    if not organisms:
        print("Could not retrieve organism list. Exiting.")
        return
        
    all_pathway_data = []
    
    print("\nStep 2 & 3: Fetching pathway data for each organism...")
    for organism in tqdm(organisms, desc="Processing organisms"):
        org_code = organism['code']
        latin_name = organism['name']
        
        pathway_data = get_pathway_data_for_organism(org_code, latin_name)
        
        if pathway_data:
            all_pathway_data.extend(pathway_data)
        
        # 每次请求后暂停更长时间
        time.sleep(REQUEST_DELAY)

    if not all_pathway_data:
        print("\nWarning: No pathway data was collected. This might be due to network issues or all target organisms lacking pathway maps.")
        return

    print(f"\nStep 4: Saving all data to {OUTPUT_FILE}...")
    df = pd.DataFrame(all_pathway_data)
    
    df = df[[
        'Organism Latin Name',
        'Level 1 Category',
        'Level 2 Category',
        'KEGG ID',
        'Pathway Name',
        'URL'
    ]]
    
    df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
    
    print(f"Done! Data for {len(df['Organism Latin Name'].unique())} organisms has been saved to {OUTPUT_FILE}.")
    print(f"Total pathways found: {len(all_pathway_data)}")

if __name__ == "__main__":
    main()