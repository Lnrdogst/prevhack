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

# Environment variables
WS_CONNECTIONS_TABLE = os.environ['WS_CONNECTIONS_TABLE']

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler for WebSocket $connect route.
    
    Registers a new WebSocket connection in the connections table
    with tenant and order information from query parameters.
    """
    try:
        # 1. Read connectionId from request context
        request_context = event.get('requestContext', {})
        connection_id = request_context.get('connectionId')
        
        if not connection_id:
            logger.error("Missing connectionId in request context")
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Missing connectionId in request context'
                })
            }
        
        # 2. Read query string parameters
        query_params = event.get('queryStringParameters') or {}
        tenant_id = query_params.get('tenantId')
        order_id = query_params.get('orderId')
        
        # 3. Validate required parameters
        if not tenant_id or not order_id:
            logger.error(f"Missing required query parameters. tenantId: {tenant_id}, orderId: {order_id}")
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Missing required query parameters: tenantId and orderId'
                })
            }
        
        # 4. Build partition key and sort key
        pk = f"{tenant_id}#{order_id}"
        sk = connection_id
        
        # Generate timestamp in ISO 8601 UTC format
        connected_at = datetime.now(timezone.utc).isoformat()
        
        # 5. Store connection in DynamoDB
        table = dynamodb.Table(WS_CONNECTIONS_TABLE)
        
        table.put_item(
            Item={
                'pk': pk,
                'sk': sk,
                'tenantId': tenant_id,
                'orderId': order_id,
                'connectionId': connection_id,
                'connectedAt': connected_at
            }
        )
        
        logger.info(f"WebSocket connection registered: {connection_id} for order {pk}")
        
        # 6. Return success response
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Connected successfully',
                'connectionId': connection_id,
                'orderId': order_id,
                'tenantId': tenant_id
            })
        }
        
    except Exception as e:
        # 7. Log error and return 500
        logger.error(f"Unexpected error in WebSocket connect: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Internal server error'
            })
        }
