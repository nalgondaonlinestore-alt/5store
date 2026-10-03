import express from 'express';
import http from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import path from 'path';
import { fileURLToPath } from 'url';
import { INITIAL_PRODUCTS } from './src/data/products.ts';
import { Product, Order, Review, WsMessage, OrderStage } from './src/types.ts';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const isProd = process.env.NODE_ENV === 'production';
const PORT = process.env.PORT || 3000;

// Authoritative in-memory state
const productsMap = new Map<string, Product>();
INITIAL_PRODUCTS.forEach(p => {
  productsMap.set(p.id, {
    ...p,
    reviews: [
      {
        id: `rev-init-${p.id}-1`,
        productId: p.id,
        userName: 'Sravani K.',
        rating: 5,
        reviewText: 'Great quality, delivered on time. Fabric and finishing match the pictures exactly!',
        createdAt: '2 days ago'
      },
      {
        id: `rev-init-${p.id}-2`,
        productId: p.id,
        userName: 'Ravi Teja',
        rating: 4,
        reviewText: 'Value for money product. Packing was secure and arrived safely.',
        createdAt: '4 days ago'
      }
    ]
  });
});

const ordersMap = new Map<string, Order>();
const clientProductViews = new Map<WebSocket, string>(); // socket -> productId
const clientsSet = new Set<WebSocket>();

function broadcast(msg: WsMessage, except?: WebSocket) {
  const data = JSON.stringify(msg);
  for (const client of clientsSet) {
    if (client !== except && client.readyState === WebSocket.OPEN) {
      try {
        client.send(data);
      } catch (err) {
        console.error('Error broadcasting to client:', err);
      }
    }
  }
}

function calculateViewers(productId: string): number {
  let count = 0;
  for (const [_, viewedId] of clientProductViews.entries()) {
    if (viewedId === productId) count++;
  }
  // Baseline simulated viewers + active socket viewers for realistic presence
  const base = productsMap.get(productId)?.viewers || 3;
  return Math.max(1, count + base);
}

const STAGES: OrderStage[] = [
  '1. Order Confirmed',
  '2. Packed & Ready',
  '3. Dispatched & In Transit',
  '4. Out for Delivery',
  '5. Delivered'
];

async function startServer() {
  const app = express();
  app.use(express.json());

  const server = http.createServer(app);
  const wss = new WebSocketServer({ server });

  // WebSockets Connection Lifecycle
  wss.on('connection', (ws: WebSocket) => {
    clientsSet.add(ws);

    // Send initial snapshot to newly connected client
    const initialPayload: WsMessage = {
      type: 'init:state',
      payload: {
        connectedDevices: clientsSet.size,
        products: Array.from(productsMap.values()),
        orders: Array.from(ordersMap.values()),
        serverTime: Date.now()
      }
    };
    ws.send(JSON.stringify(initialPayload));

    // Broadcast updated connected count to all clients
    broadcast({
      type: 'presence:update',
      payload: { connectedDevices: clientsSet.size }
    });

    ws.on('message', (messageBuffer) => {
      try {
        const msg: WsMessage = JSON.parse(messageBuffer.toString());
        handleWsMessage(ws, msg);
      } catch (e) {
        console.error('Failed to parse WS message:', e);
      }
    });

    ws.on('close', () => {
      const viewedProdId = clientProductViews.get(ws);
      clientProductViews.delete(ws);
      clientsSet.delete(ws);

      // Broadcast viewer count decrease if viewing a product
      if (viewedProdId) {
        broadcast({
          type: 'product:viewers_updated',
          payload: {
            productId: viewedProdId,
            viewers: calculateViewers(viewedProdId)
          }
        });
      }

      broadcast({
        type: 'presence:update',
        payload: { connectedDevices: clientsSet.size }
      });
    });
  });

  function handleWsMessage(ws: WebSocket, msg: WsMessage) {
    switch (msg.type) {
      case 'product:view': {
        const productId = msg.payload?.productId;
        if (productId) {
          clientProductViews.set(ws, productId);
          broadcast({
            type: 'product:viewers_updated',
            payload: {
              productId,
              viewers: calculateViewers(productId)
            }
          });
        }
        break;
      }

      case 'product:unview': {
        const prevId = clientProductViews.get(ws);
        clientProductViews.delete(ws);
        if (prevId) {
          broadcast({
            type: 'product:viewers_updated',
            payload: {
              productId: prevId,
              viewers: calculateViewers(prevId)
            }
          });
        }
        break;
      }

      case 'stock:adjust': {
        const { productId, delta, absolute } = msg.payload || {};
        const product = productsMap.get(productId);
        if (product) {
          if (typeof absolute === 'number') {
            product.stock = Math.max(0, absolute);
          } else if (typeof delta === 'number') {
            product.stock = Math.max(0, product.stock + delta);
          }
          const updatedEvent: WsMessage = {
            type: 'stock:updated',
            payload: {
              productId,
              stock: product.stock,
              inStock: product.stock > 0,
              title: product.title,
              reason: msg.payload?.reason || 'manual'
            }
          };
          broadcast(updatedEvent);
        }
        break;
      }

      case 'order:place': {
        const orderData = msg.payload as Order;
        if (orderData && orderData.orderId) {
          // Verify & decrement stock authoritatively
          for (const item of orderData.items) {
            const product = productsMap.get(item.productId);
            if (product) {
              product.stock = Math.max(0, product.stock - (item.qty || 1));
              broadcast({
                type: 'stock:updated',
                payload: {
                  productId: product.id,
                  stock: product.stock,
                  inStock: product.stock > 0,
                  title: product.title,
                  reason: 'purchase'
                }
              });
            }
          }

          ordersMap.set(orderData.orderId, orderData);
          broadcast({
            type: 'order:created',
            payload: orderData
          });

          // Broadcast activity feed notification
          broadcast({
            type: 'activity:feed',
            payload: {
              text: `${orderData.customerName || 'A customer'} from ${orderData.city || 'India'} just placed an order!`,
              time: Date.now()
            }
          });
        }
        break;
      }

      case 'order:advance_stage': {
        const { orderId } = msg.payload || {};
        const order = ordersMap.get(orderId);
        if (order) {
          const currentIndex = STAGES.indexOf(order.trackingStage);
          if (currentIndex < STAGES.length - 1) {
            order.trackingStage = STAGES[currentIndex + 1];
            order.status = order.trackingStage;
            broadcast({
              type: 'order:status_updated',
              payload: {
                orderId: order.orderId,
                trackingStage: order.trackingStage,
                status: order.status
              }
            });
          }
        }
        break;
      }

      case 'order:cancel': {
        const { orderId } = msg.payload || {};
        const order = ordersMap.get(orderId);
        if (order && order.canCancel) {
          order.canCancel = false;
          order.status = 'Cancelled by Customer';
          order.cancellationStatus = '1.User canceled after paid';
          
          // Restock items
          for (const item of order.items) {
            const p = productsMap.get(item.productId);
            if (p) {
              p.stock += item.qty || 1;
              broadcast({
                type: 'stock:updated',
                payload: {
                  productId: p.id,
                  stock: p.stock,
                  inStock: p.stock > 0,
                  title: p.title,
                  reason: 'restock_cancel'
                }
              });
            }
          }

          broadcast({
            type: 'order:cancelled',
            payload: {
              orderId,
              status: order.status,
              cancellationStatus: order.cancellationStatus
            }
          });
        }
        break;
      }

      case 'review:submit': {
        const { productId, rating, reviewText, userName, orderId } = msg.payload || {};
        const product = productsMap.get(productId);
        if (product && rating && reviewText) {
          const newReview: Review = {
            id: `rev-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
            productId,
            orderId,
            userName: userName || 'Verified Buyer',
            rating: Number(rating),
            reviewText,
            createdAt: 'Just now'
          };
          product.reviews = [newReview, ...(product.reviews || [])];
          product.reviewCount = product.reviews.length;
          const sum = product.reviews.reduce((acc, r) => acc + r.rating, 0);
          product.rating = Number((sum / product.reviews.length).toFixed(1));

          broadcast({
            type: 'review:added',
            payload: {
              productId,
              review: newReview,
              rating: product.rating,
              reviewCount: product.reviewCount
            }
          });

          broadcast({
            type: 'activity:feed',
            payload: {
              text: `New ${rating}★ review added for "${product.title.slice(0, 30)}..."`,
              time: Date.now()
            }
          });
        }
        break;
      }

      case 'ping': {
        ws.send(JSON.stringify({ type: 'pong', timestamp: Date.now() }));
        break;
      }
    }
  }

  // REST endpoints for data queries and fallback integration
  app.get('/api/products', (req, res) => {
    res.json({ success: true, products: Array.from(productsMap.values()) });
  });

  app.get('/api/products/:id', (req, res) => {
    const product = productsMap.get(req.params.id);
    if (!product) return res.status(404).json({ success: false, message: 'Product not found' });
    res.json({ success: true, product });
  });

  app.get('/api/orders', (req, res) => {
    const email = req.query.email as string;
    let list = Array.from(ordersMap.values());
    if (email) {
      list = list.filter(o => o.email.toLowerCase() === email.toLowerCase());
    }
    res.json({ success: true, orders: list });
  });

  app.post('/api/orders', (req, res) => {
    const orderData = req.body as Order;
    if (!orderData || !orderData.orderId) {
      return res.status(400).json({ success: false, message: 'Invalid order payload' });
    }
    // Update authoritative stock
    for (const item of orderData.items || []) {
      const product = productsMap.get(item.productId);
      if (product) {
        product.stock = Math.max(0, product.stock - (item.qty || 1));
        broadcast({
          type: 'stock:updated',
          payload: {
            productId: product.id,
            stock: product.stock,
            inStock: product.stock > 0,
            title: product.title,
            reason: 'purchase'
          }
        });
      }
    }
    ordersMap.set(orderData.orderId, orderData);
    broadcast({ type: 'order:created', payload: orderData });
    res.json({ success: true, order: orderData });
  });

  // Simulation test endpoint
  app.post('/api/simulate/stock-decrement', (req, res) => {
    const { productId } = req.body;
    const product = productsMap.get(productId);
    if (!product) return res.status(404).json({ success: false, message: 'Product not found' });
    product.stock = Math.max(0, product.stock - 1);
    broadcast({
      type: 'stock:updated',
      payload: {
        productId: product.id,
        stock: product.stock,
        inStock: product.stock > 0,
        title: product.title,
        reason: 'simulated_purchase'
      }
    });
    res.json({ success: true, stock: product.stock });
  });

  app.post('/api/simulate/restock', (req, res) => {
    const { productId, amount = 10 } = req.body;
    const product = productsMap.get(productId);
    if (!product) return res.status(404).json({ success: false, message: 'Product not found' });
    product.stock += amount;
    broadcast({
      type: 'stock:updated',
      payload: {
        productId: product.id,
        stock: product.stock,
        inStock: true,
        title: product.title,
        reason: 'restocked'
      }
    });
    res.json({ success: true, stock: product.stock });
  });

  app.get('/api/status', (req, res) => {
    res.json({
      success: true,
      connectedClients: clientsSet.size,
      productCount: productsMap.size,
      orderCount: ordersMap.size,
      serverTime: Date.now()
    });
  });

  // Vite middleware in dev or static files in production
  if (!isProd) {
    const { createServer: createViteServer } = await import('vite');
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  } else {
    app.use(express.static(path.resolve(__dirname, 'dist')));
    app.get('*', (req, res) => {
      res.sendFile(path.resolve(__dirname, 'dist', 'index.html'));
    });
  }

  server.listen(Number(PORT), '0.0.0.0', () => {
    console.log(`Server and WebSockets running on port ${PORT} (isProd: ${isProd})`);
  });
}

startServer().catch(err => {
  console.error('Fatal server startup error:', err);
  process.exit(1);
});
