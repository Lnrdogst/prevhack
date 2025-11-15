import json
import boto3
import os
import logging
from datetime import datetime, timezone
from typing import Dict, Any

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
eventbridge = boto3.client('events')

# Environment variables
ORDERS_TABLE = os.environ['ORDERS_TABLE']
ORDER_STATUS_CHANGED_DETAIL_TYPE = os.environ['ORDER_STATUS_CHANGED_DETAIL_TYPE']
ORDER_STATUS_CHANGED_SOURCE = os.environ['ORDER_STATUS_CHANGED_SOURCE']
REGION = os.environ['REGION']

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler for updating order status.
    
    Parses the orderId from path parameters, updates the order in DynamoDB,
    and publishes an event to EventBridge.
    """
    try:
        # 1. Parse orderId from path parameters
        path_parameters = event.get('pathParameters')
        if not path_parameters or not path_parameters.get('orderId'):
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json'
                },
                'body': json.dumps({
                    'error': 'Missing orderId in path parameters'
                })
            }
        
        order_id = path_parameters['orderId']
        
        # 2. Parse JSON body
        try:
            body = json.loads(event.get('body', '{}'))
        except json.JSONDecodeError:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json'
                },
                'body': json.dumps({
                    'error': 'Invalid JSON body'
                })
            }
        
        # Validate required fields
        tenant_id = body.get('tenantId')
        status = body.get('status')
        
        if not tenant_id or not status:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json'
                },
                'body': json.dumps({
                    'error': 'Missing required fields: tenantId and status'
                })
            }
        
        # Validate data types
        if not isinstance(tenant_id, str) or not isinstance(status, str):
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json'
                },
                'body': json.dumps({
                    'error': 'tenantId and status must be strings'
                })
            }
        
        # 3. Build partition key
        pk = f"{tenant_id}#{order_id}"
        
        # 4. Generate timestamp in ISO 8601 UTC format
        updated_at = datetime.now(timezone.utc).isoformat()
        
        # 5. Upsert order in DynamoDB
        table = dynamodb.Table(ORDERS_TABLE)
        
        table.put_item(
            Item={
                'pk': pk,
                'tenantId': tenant_id,
                'orderId': order_id,
                'status': status,
                'updatedAt': updated_at
            }
        )
        
        logger.info(f"Order updated: {pk}, status: {status}")
        
        # 6. Publish EventBridge event
        event_detail = {
            'tenantId': tenant_id,
            'orderId': order_id,
            'status': status,
            'updatedAt': updated_at
        }
        
        eventbridge.put_events(
            Entries=[
                {
                    'Source': ORDER_STATUS_CHANGED_SOURCE,
                    'DetailType': ORDER_STATUS_CHANGED_DETAIL_TYPE,
                    'Detail': json.dumps(event_detail),
                    'Region': REGION
                }
            ]
        )
        
        logger.info(f"EventBridge event published for order: {order_id}")
        
        # 7. Return success response
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'message': 'status updated',
                'orderId': order_id,
                'status': status
            })
        }
        
    except Exception as e:
        # 8. Log error and return 500
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'error': 'Internal server error'
            })
        }
