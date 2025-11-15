import json
import boto3
import os
import logging
from typing import Dict, Any
from boto3.dynamodb.conditions import Key

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')

# Environment variables
WS_CONNECTIONS_TABLE = os.environ['WS_CONNECTIONS_TABLE']

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler for WebSocket $disconnect route.
    
    Removes the WebSocket connection from the connections table
    by first finding it via GSI and then deleting from the base table.
    """
    try:
        # 1. Read connectionId from request context
        request_context = event.get('requestContext', {})
        connection_id = request_context.get('connectionId')
        
        if not connection_id:
            logger.warning("Missing connectionId in request context")
            # Return 200 even if missing to avoid retries
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Disconnect processed (no connectionId found)'
                })
            }
        
        logger.info(f"Processing disconnect for connectionId: {connection_id}")
        
        # 2. Query the GSI "SkIndex" to find the connection by sk = connectionId
        table = dynamodb.Table(WS_CONNECTIONS_TABLE)
        
        response = table.query(
            IndexName='SkIndex',
            KeyConditionExpression=Key('sk').eq(connection_id),
            Limit=1
        )
        
        items = response.get('Items', [])
        
        if not items:
            logger.info(f"Connection not found in table for connectionId: {connection_id}")
            # Return 200 even if not found to avoid retries
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Disconnect processed (connection not found)',
                    'connectionId': connection_id
                })
            }
        
        # 3. Delete the item from the base table using pk and sk
        item = items[0]
        pk = item['pk']
        sk = item['sk']
        
        table.delete_item(
            Key={
                'pk': pk,
                'sk': sk
            }
        )
        
        logger.info(f"Successfully deleted connection: pk={pk}, sk={sk}")
        
        # 4. Always return 200 to avoid retries
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Disconnected successfully',
                'connectionId': connection_id,
                'pk': pk
            })
        }
        
    except Exception as e:
        # 5. Log error but still return 200 to avoid retries
        logger.error(f"Error processing disconnect for connectionId {connection_id if 'connection_id' in locals() else 'unknown'}: {str(e)}", exc_info=True)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Disconnect processed with error',
                'error': str(e)
            })
        }
