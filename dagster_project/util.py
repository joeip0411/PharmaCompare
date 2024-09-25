import os

import boto3

ECS_CLIENT = boto3.client(
    'ecs',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name = 'ap-southeast-2',
)

ECS_CLUSTER_NAME = "ecs_cluster"
ECS_TASK_DEFINITION = "cw_price_scraping"
SUBNETS = ["subnet-0d1e2fce04364958d", 
           "subnet-00db28f8b82b2d34f", 
           "subnet-06a808b04ebb6b730" ]
SECURITY_GROUPS = ["sg-03cf555952fc36384"]
PRICE_CONTAINER_OVERRIDES = {
    'name': 'cw_price_scrap',
    'command': ["product_price.py"],
    'environment':[
        {
            "name":"PRODUCT_PRICE_TABLE",
            "value":os.getenv('PRODUCT_PRICE_TABLE'),
        },
        {
            "name":"SUPABASE_KEY",
            "value":os.getenv('SUPABASE_KEY'),
        },
        {
            "name":"SUPABASE_URL",
            "value":os.getenv('SUPABASE_URL'),
        }, 
    ],
}
PRODUCT_CONTAINER_OVERRIDES = {
    'name': 'cw_price_scrap',
    'command': ["product_desc.py"], 
    'environment':[
        {
            "name":"PRODUCT_DESC_TABLE",
            "value":os.getenv('PRODUCT_DESC_TABLE'),
        },
        {
            "name":"SUPABASE_KEY",
            "value":os.getenv('SUPABASE_KEY'),
        },
        {
            "name":"SUPABASE_URL",
            "value":os.getenv('SUPABASE_URL'),
        }, 
    ],
}
NETWORK_CONFIGURATION = {
    'awsvpcConfiguration': {
        'subnets': SUBNETS,
        'securityGroups': SECURITY_GROUPS,
        'assignPublicIp': 'ENABLED',
    },
}

