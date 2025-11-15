import json
import boto3
import os
import logging
from typing import Dict, Any, List
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')

# Environment variables
WS_CONNECTIONS_TABLE = os.environ['WS_CONNECTIONS_TABLE']
WS_API_ID = os.environ['WS_API_ID']
REGION = os.environ['REGION']
STAGE = os.environ['STAGE']

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler for broadcasting order status changes to WebSocket connections.
    
    Triggered by EventBridge when an order status changes. Finds all WebSocket
    connections for the order and sends the update to each client.
    """
    try:
        # 1. Read detail from EventBridge event
        detail = event.get('detail', {})
        tenant_id = detail.get('tenantId')
        order_id = detail.get('orderId')
        
        if not tenant_id or not order_id:
            logger.error(f"Missing required fields in event detail: {detail}")
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Missing tenantId or orderId in event detail'
                })
            }
        
        logger.info(f"Broadcasting status change for order: {tenant_id}#{order_id}")
        
        # 2. Build partition key
        pk = f"{tenant_id}#{order_id}"
        
        # 3. Query WS_CONNECTIONS_TABLE for all connections with this pk
        table = dynamodb.Table(WS_CONNECTIONS_TABLE)
        
        response = table.query(
            KeyConditionExpression=Key('pk').eq(pk)
        )
        
        connections = response.get('Items', [])
        logger.info(f"Found {len(connections)} connections for order {pk}")
        
        if not connections:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No connections found for this order',
                    'orderId': order_id,
                    'tenantId': tenant_id
                })
            }
        
        # Initialize API Gateway Management API client
        # Build the endpoint URL
        endpoint_url = f"https://{WS_API_ID}.execute-api.{REGION}.amazonaws.com/{STAGE}"
        apigw_client = boto3.client(
            'apigatewaymanagementapi',
            endpoint_url=endpoint_url,
            region_name=REGION
        )
        
        # Prepare the message data
        message_data = json.dumps(detail).encode('utf-8')
        
        # Track results
        successful_sends = 0
        failed_sends = 0
        stale_connections_removed = 0
        
        # 4. Send message to each connection
        for connection in connections:
            connection_id = connection['sk']
            
            try:
                apigw_client.post_to_connection(
                    ConnectionId=connection_id,
                    Data=message_data
                )
                successful_sends += 1
                logger.debug(f"Message sent successfully to connection: {connection_id}")
                
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code')
                
                # 5. Handle 410 Gone - connection is stale
                if error_code == 'GoneException':
                    logger.info(f"Removing stale connection: {connection_id}")
                    
                    try:
                        table.delete_item(
                            Key={
                                'pk': pk,
                                'sk': connection_id
                            }
                        )
                        stale_connections_removed += 1
                        logger.info(f"Stale connection removed: {connection_id}")
                        
                    except Exception as delete_error:
                        logger.error(f"Failed to delete stale connection {connection_id}: {delete_error}")
                
                else:
                    # Other API Gateway errors
                    logger.error(f"Failed to send message to connection {connection_id}: {e}")
                    failed_sends += 1
                    
            except Exception as e:
                logger.error(f"Unexpected error sending to connection {connection_id}: {e}")
                failed_sends += 1
        
        # 6. Log summary
        logger.info(f"Broadcast completed for order {pk}: {successful_sends} successful, "
                   f"{failed_sends} failed, {stale_connections_removed} stale connections removed")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Broadcast completed',
                'orderId': order_id,
                'tenantId': tenant_id,
                'connectionsFound': len(connections),
                'successfulSends': successful_sends,
                'failedSends': failed_sends,
                'staleConnectionsRemoved': stale_connections_removed
            })
        }
        
    except Exception as e:
        logger.error(f"Unexpected error in broadcast handler: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Internal server error',
                'message': str(e)
            })
        }
