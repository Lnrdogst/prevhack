# API Pedidos Restaurante

Sistema serverless de gestión de pedidos en tiempo real usando AWS Lambda, API Gateway, WebSockets y EventBridge.

## Arquitectura

### Componentes Principales

- **HTTP API Gateway**: Endpoint REST para actualización de estados
- **WebSocket API Gateway**: Comunicación bidireccional en tiempo real
- **AWS Lambda**: 4 funciones para lógica de negocio
- **EventBridge**: Bus de eventos para desacoplamiento
- **DynamoDB**: Base de datos NoSQL con 2 tablas

### Funciones Lambda

1. **applyStatus** (`POST /orders/{orderId}/status`)
   - Actualiza estado del pedido en DynamoDB
   - Publica evento `OrderStatusChanged` en EventBridge

2. **onConnect** (WebSocket `$connect`)
   - Registra nuevas conexiones WebSocket por tenant/orden
   - Query params requeridos: `tenantId`, `orderId`

3. **onDisconnect** (WebSocket `$disconnect`)
   - Limpia conexiones desconectadas usando GSI
   - Manejo automático de cleanup

4. **broadcast** (EventBridge trigger)
   - Envía actualizaciones a todos los clientes conectados
   - Auto-cleanup de conexiones inválidas (410 Gone)

### Flujo de Datos

```
Operador → HTTP API → applyStatus → DynamoDB + EventBridge
                                        ↓
Clientes ← WebSocket ← broadcast ← EventBridge
```

1. **Actualización**: Operador llama HTTP API
2. **Persistencia**: Se guarda en tabla Orders
3. **Evento**: EventBridge publica cambio
4. **Broadcast**: Función encuentra conexiones y envía updates
5. **Tiempo Real**: Clientes reciben notificación instantánea

### Esquema DynamoDB

**OrdersTable**
- `pk`: `tenantId#orderId` (Partition Key)
- Atributos: `tenantId`, `orderId`, `status`, `updatedAt`

**WSConnectionsTable**
- `pk`: `tenantId#orderId` (Partition Key) 
- `sk`: `connectionId` (Sort Key)
- **GSI "SkIndex"**: `sk` como partition key, projección ALL
- Atributos: `tenantId`, `orderId`, `connectionId`, `connectedAt`

### Variables de Entorno

```bash
# Tablas
ORDERS_TABLE=api-pedidos-restaurante-orders-{stage}
WS_CONNECTIONS_TABLE=api-pedidos-restaurante-ws-connections-{stage}

# EventBridge
ORDER_STATUS_CHANGED_SOURCE=orders.service
ORDER_STATUS_CHANGED_DETAIL_TYPE=OrderStatusChanged

# WebSocket
WS_API_ID={WebsocketsApi CloudFormation Ref}
REGION=us-east-1
STAGE={stage}
```

### Uso

**Conectar WebSocket:**
```javascript
const ws = new WebSocket(
  'wss://api-id.execute-api.us-east-1.amazonaws.com/dev?tenantId=resto123&orderId=order456'
);
```

**Actualizar Estado:**
```bash
curl -X POST 'https://api-id.execute-api.us-east-1.amazonaws.com/dev/orders/order456/status' \
  -H 'Content-Type: application/json' \
  -d '{
    "tenantId": "resto123",
    "status": "preparando"
  }'
```

**Evento recibido:**
```json
{
  "tenantId": "resto123",
  "orderId": "order456", 
  "status": "preparando",
  "updatedAt": "2025-11-14T15:30:00.000Z"
}
```

### Despliegue

```bash
# Instalar dependencias
npm install -g serverless

# Deploy
serverless deploy --stage dev
```