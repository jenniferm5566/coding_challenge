import json
import pandas as pd
from datetime import datetime


# load json
def load_json(file_path):
    data = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data.append(json.loads(line.strip()))
                except json.JSONDecodeError as e:
                    print(f"Error decoding line: {line.strip()} - {e}")
    except FileNotFoundError:
        print("File not found.")
    return data


def convert_date(timestamp_ms):
    if timestamp_ms is not None:
        return datetime.fromtimestamp(timestamp_ms/1000)
    
    return timestamp_ms


# Convert Users data to table
user_data = load_json('data/users.json')
df_users = pd.DataFrame(columns=['id', 'active', 'createdDate', 'lastLogin', 'role', 'signUpSource', 'state'])
for row in user_data:
    if 'lastLogin' in row:
        lastLogin = row['lastLogin']['$date']
    else:
        lastLogin = None
    
    data = [row['_id']['$oid'], row['active'], row['createdDate']['$date'], lastLogin, row['role'], row.get('signUpSource', None), row.get('state', None)]
    df_users.loc[len(df_users)] = data

df_users['createdDate'] = df_users['createdDate'].apply(convert_date)
df_users['lastLogin'] = df_users['lastLogin'].apply(convert_date)


# Convert Brands data to table
brand_data = load_json('data/brands.json')
df_brands = pd.DataFrame(columns=['id', 'barcode', 'brandCode', 'categoryCode', 'cpgId', 'name', 'topBrand'])
for row in brand_data:
    data = [row['_id']['$oid'], row['barcode'], row.get('brandCode', None), row.get('categoryCode', None), row['cpg']['$id']['$oid'], row['name'], row.get('topBrand', None)]
    df_brands.loc[len(df_brands)] = data


brand_codes = list(df_brands['brandCode'])
ids = list(df_brands['id'])
brand_code_id_dict = {}
for i in range(len(brand_codes)):
    if brand_codes[i] is not None:
        brand_code_id_dict[brand_codes[i]] = ids[i]


# Convert Receipt data to table
receipts_data = load_json('data/receipts.json')
df_receipts = pd.DataFrame(columns=['id', 'createDate', 'dateScanned', 'finishedDate', 
                                    'modifyDate', 'pointsAwardedDate', 'purchaseDate',
                                    'pointsEarned', 'bonusPointsEarned', 'bonusPointsEarnedReason', 'purchasedItemCount', 
                                    'rewardsReceiptStatus', 'totalSpent', 'userId'])
for row in receipts_data:
    if 'finishedDate' in row:
        finished_date = row['finishedDate']['$date']
    else:
        finished_date = None

    if 'pointsAwardedDate' in row:
        points_awarded_date = row['pointsAwardedDate']['$date']
    else:
        points_awarded_date = None

    if 'purchaseDate' in row:
        purchase_date = row['purchaseDate']['$date']
    else:
        purchase_date = None

    data = [row['_id']['$oid'], row['createDate']['$date'], row['dateScanned']['$date'], finished_date,
            row['modifyDate']['$date'], points_awarded_date, purchase_date, row.get('pointsEarned', None),
            row.get('bonusPointsEarned', None), row.get('bonusPointsEarnedReason', None), row.get('purchasedItemCount', None), 
            row['rewardsReceiptStatus'], row.get('totalSpent', None), row['userId']]
    df_receipts.loc[len(df_receipts)] = data

df_receipts['createDate'] = df_receipts['createDate'].apply(convert_date)
df_receipts['dateScanned'] = df_receipts['dateScanned'].apply(convert_date)
df_receipts['finishedDate'] = df_receipts['finishedDate'].apply(convert_date)
df_receipts['modifyDate'] = df_receipts['modifyDate'].apply(convert_date)
df_receipts['pointsAwardedDate'] = df_receipts['pointsAwardedDate'].apply(convert_date)
df_receipts['purchaseDate'] = df_receipts['purchaseDate'].apply(convert_date)



# Get bonus Points Earned Reasons dict
bonusPointsEarnedReasons = df_receipts['bonusPointsEarnedReason'].unique()
bonusPointsEarnedReasonsDict = {}
for i in range(len(bonusPointsEarnedReasons)):
    bonusPointsEarnedReasonsDict[bonusPointsEarnedReasons[i]] = i + 1
df_receipts['bonusPointsEarnedReason'] = df_receipts['bonusPointsEarnedReason'].apply(lambda x: bonusPointsEarnedReasonsDict[x])



# Items table and RewardsReceiptsItems table
df_items = pd.DataFrame(columns=['barcode', 'brandId', 'description', 'itemPrice', 'deleted', 'competitiveProduct'])
df_reward_receipt_items = pd.DataFrame(columns=['receiptsId', 'itemId', 'discountedItemPrice', 'priceAfterCoupon', 'finalPrice', 'itemPurchased', 
                                                'needsFetchReview', 'needsFetchReviewReason', 'pointsEarned', 'metabriteCampaignId', 'preventTargetGapPoints'])
barcode_seen = set()
for row in receipts_data:
    receiptsId = row['_id']['$oid']
    item_list = row.get('rewardsReceiptItemList', [])
    for item_info in item_list:
        barcode = item_info.get('barcode')
        if barcode is not None and barcode not in barcode_seen:
            barcode_seen.add(barcode)
            if 'brandCode' in item_info:
                brand_id = brand_code_id_dict.get(item_info['brandCode'], None)
            else:
                brand_id = None
            description = item_info.get('description')
            itemPrice = item_info.get('description')
            deleted = item_info.get('deleted')
            competitiveProduct = item_info.get('competitiveProduct')
            itemPrice = item_info.get('itemPrice')
            df_items.loc[len(df_items)] = [barcode, brand_id, description, itemPrice, deleted, competitiveProduct]

        if barcode is not None:
            curr_idx = df_items.index[df_items['barcode'] == barcode].tolist()[0]
            discountedItemPrice = item_info.get('discountedItemPrice')
            priceAfterCoupon = item_info.get('priceAfterCoupon')
            finalPrice = item_info.get('finalPrice')
            itemPurchased = item_info.get('itemPurchased')
            needsFetchReview = item_info.get('needsFetchReview')
            needsFetchReviewReason = item_info.get('needsFetchReviewReason')
            pointsEarned = item_info.get('pointsEarned')
            metabriteCampaignId = item_info.get('metabriteCampaignId')
            preventTargetGapPoints = item_info.get('preventTargetGapPoints')
            df_reward_receipt_items.loc[len(df_reward_receipt_items)] = [receiptsId, curr_idx, discountedItemPrice, priceAfterCoupon, finalPrice, itemPurchased,
                                                                        needsFetchReview, needsFetchReviewReason, pointsEarned, metabriteCampaignId, preventTargetGapPoints]
            


# Save tables
df_users.to_csv("data/df_users.csv")
df_brands.to_csv("data/df_brands.csv")
df_receipts.to_csv("data/df_receipts.csv")
df_items.to_csv("data/df_items.csv")
df_reward_receipt_items.to_csv("data/df_reward_receipt_items.csv")




