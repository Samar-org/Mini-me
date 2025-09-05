// server.js - Backend Server with Airtable as Primary Database
const express = require('express');
const cors = require('cors');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const axios = require('axios');
const multer = require('multer');
const path = require('path');
const Airtable = require('airtable');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json());
app.use('/uploads', express.static('uploads'));

// ============= AIRTABLE CONFIGURATION =============
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

// Table Names
const TABLES = {
  USERS: 'Users',
  PRODUCTS: 'Products',
  SCAN_HISTORY: 'ScanHistory',
  SETTINGS: 'Settings'
};

// ============= FILE UPLOAD CONFIGURATION =============
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, 'uploads/');
  },
  filename: (req, file, cb) => {
    cb(null, Date.now() + path.extname(file.originalname));
  }
});

const upload = multer({ 
  storage,
  limits: { fileSize: 10 * 1024 * 1024 }, // 10MB
  fileFilter: (req, file, cb) => {
    const allowedTypes = /jpeg|jpg|png|gif|webp/;
    const extname = allowedTypes.test(path.extname(file.originalname).toLowerCase());
    const mimetype = allowedTypes.test(file.mimetype);
    
    if (mimetype && extname) {
      return cb(null, true);
    } else {
      cb(new Error('Only image files are allowed'));
    }
  }
});

// ============= HELPER FUNCTIONS =============

// Convert Airtable record to clean object
function cleanRecord(record) {
  return {
    id: record.id,
    ...record.fields
  };
}

// Find record by field
async function findRecordByField(tableName, fieldName, value) {
  try {
    const records = await base(tableName)
      .select({
        filterByFormula: `{${fieldName}} = '${value}'`,
        maxRecords: 1
      })
      .firstPage();
    
    return records.length > 0 ? cleanRecord(records[0]) : null;
  } catch (error) {
    console.error(`Error finding record in ${tableName}:`, error);
    return null;
  }
}

// Create record
async function createRecord(tableName, fields) {
  try {
    const record = await base(tableName).create(fields);
    return cleanRecord(record);
  } catch (error) {
    console.error(`Error creating record in ${tableName}:`, error);
    throw error;
  }
}

// Update record
async function updateRecord(tableName, recordId, fields) {
  try {
    const record = await base(tableName).update(recordId, fields);
    return cleanRecord(record);
  } catch (error) {
    console.error(`Error updating record in ${tableName}:`, error);
    throw error;
  }
}

// ============= AUTHENTICATION MIDDLEWARE =============
const authenticateToken = async (req, res, next) => {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];

  if (!token) {
    return res.status(401).json({ error: 'Access token required' });
  }

  try {
    const decoded = jwt.verify(token, process.env.JWT_SECRET || 'your-secret-key');
    
    // Get user from Airtable
    const user = await findRecordByField(TABLES.USERS, 'Email', decoded.email);
    if (!user) {
      return res.status(403).json({ error: 'User not found' });
    }
    
    req.user = user;
    next();
  } catch (err) {
    return res.status(403).json({ error: 'Invalid token' });
  }
};

// ============= AUTH ROUTES =============

// Register
app.post('/api/auth/register', async (req, res) => {
  try {
    const { email, password, name } = req.body;
    
    // Check if user exists
    const existingUser = await findRecordByField(TABLES.USERS, 'Email', email);
    if (existingUser) {
      return res.status(400).json({ error: 'User already exists' });
    }
    
    // Hash password
    const hashedPassword = await bcrypt.hash(password, 10);
    
    // Create user in Airtable
    const user = await createRecord(TABLES.USERS, {
      Email: email,
      Password: hashedPassword,
      Name: name,
      Role: 'staff',
      CreatedAt: new Date().toISOString(),
      TotalScans: 0,
      LastActive: new Date().toISOString()
    });
    
    // Generate token
    const token = jwt.sign(
      { id: user.id, email: user.Email },
      process.env.JWT_SECRET || 'your-secret-key',
      { expiresIn: '30d' }
    );
    
    res.json({
      token,
      user: {
        id: user.id,
        email: user.Email,
        name: user.Name,
        role: user.Role
      }
    });
  } catch (error) {
    console.error('Registration error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Login
app.post('/api/auth/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    
    // Find user
    const user = await findRecordByField(TABLES.USERS, 'Email', email);
    if (!user) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }
    
    // Check password
    const validPassword = await bcrypt.compare(password, user.Password);
    if (!validPassword) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }
    
    // Update last active
    await updateRecord(TABLES.USERS, user.id, {
      LastActive: new Date().toISOString()
    });
    
    // Generate token
    const token = jwt.sign(
      { id: user.id, email: user.Email },
      process.env.JWT_SECRET || 'your-secret-key',
      { expiresIn: '30d' }
    );
    
    res.json({
      token,
      user: {
        id: user.id,
        email: user.Email,
        name: user.Name,
        role: user.Role
      }
    });
  } catch (error) {
    console.error('Login error:', error);
    res.status(500).json({ error: error.message });
  }
});

// ============= PRODUCT LOOKUP APIS =============

// Lookup product from multiple sources
app.post('/api/products/lookup', authenticateToken, async (req, res) => {
  try {
    const { barcode, provider } = req.body;
    let productData = null;
    
    switch(provider) {
      case 'openFood':
        productData = await lookupOpenFoodFacts(barcode);
        break;
      case 'upcItemDB':
        productData = await lookupUPCItemDB(barcode);
        break;
      case 'barcodeLookup':
        productData = await lookupBarcodeLookup(barcode);
        break;
      case 'all':
        // Try all providers
        productData = await lookupOpenFoodFacts(barcode) ||
                     await lookupUPCItemDB(barcode) ||
                     await lookupBarcodeLookup(barcode);
        break;
      default:
        productData = await lookupOpenFoodFacts(barcode);
    }
    
    if (productData) {
      res.json({ success: true, data: productData });
    } else {
      res.json({ success: false, message: 'Product not found' });
    }
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Open Food Facts API
async function lookupOpenFoodFacts(barcode) {
  try {
    const response = await axios.get(`https://world.openfoodfacts.org/api/v0/product/${barcode}.json`);
    
    if (response.data.status === 1 && response.data.product) {
      const product = response.data.product;
      return {
        barcode,
        name: product.product_name || 'Unknown Product',
        brand: product.brands || '',
        category: product.categories || '',
        description: product.generic_name || '',
        images: product.image_url ? [product.image_url] : [],
        weight: product.quantity || '',
        source: 'Open Food Facts'
      };
    }
  } catch (error) {
    console.error('Open Food Facts error:', error.message);
  }
  return null;
}

// UPC ItemDB API
async function lookupUPCItemDB(barcode) {
  if (!process.env.UPC_ITEMDB_KEY) return null;
  
  try {
    const response = await axios.get('https://api.upcitemdb.com/prod/trial/lookup', {
      params: { upc: barcode }
    });
    
    if (response.data.items && response.data.items.length > 0) {
      const item = response.data.items[0];
      return {
        barcode,
        name: item.title || 'Unknown Product',
        brand: item.brand || '',
        category: item.category || '',
        description: item.description || '',
        images: item.images || [],
        weight: item.weight || '',
        dimensions: item.dimension || '',
        price: item.lowest_recorded_price || null,
        source: 'UPC ItemDB'
      };
    }
  } catch (error) {
    console.error('UPC ItemDB error:', error.message);
  }
  return null;
}

// Barcode Lookup API
async function lookupBarcodeLookup(barcode) {
  if (!process.env.BARCODE_LOOKUP_KEY) return null;
  
  try {
    const response = await axios.get('https://api.barcodelookup.com/v3/products', {
      params: {
        barcode,
        key: process.env.BARCODE_LOOKUP_KEY
      }
    });
    
    if (response.data.products && response.data.products.length > 0) {
      const product = response.data.products[0];
      return {
        barcode,
        name: product.title || 'Unknown Product',
        brand: product.brand || '',
        category: product.category || '',
        description: product.description || '',
        images: product.images || [],
        weight: product.weight || '',
        dimensions: `${product.length || ''} x ${product.width || ''} x ${product.height || ''}`,
        price: product.stores?.[0]?.price || null,
        source: 'Barcode Lookup'
      };
    }
  } catch (error) {
    console.error('Barcode Lookup error:', error.message);
  }
  return null;
}

// ============= PRODUCT ROUTES =============

// Get all products
app.get('/api/products', authenticateToken, async (req, res) => {
  try {
    const { page = 1, limit = 50, search, status, category } = req.query;
    
    let filterFormula = '';
    const filters = [];
    
    if (search) {
      filters.push(`OR(SEARCH('${search}', LOWER({Name})), SEARCH('${search}', {Barcode}))`);
    }
    if (status) {
      filters.push(`{Status} = '${status}'`);
    }
    if (category) {
      filters.push(`{Category} = '${category}'`);
    }
    
    if (filters.length > 0) {
      filterFormula = filters.length > 1 ? `AND(${filters.join(', ')})` : filters[0];
    }
    
    const records = await base(TABLES.PRODUCTS)
      .select({
        maxRecords: parseInt(limit),
        pageSize: parseInt(limit),
        sort: [{ field: 'ScannedAt', direction: 'desc' }],
        filterByFormula
      })
      .firstPage();
    
    const products = records.map(cleanRecord);
    
    res.json({
      products,
      currentPage: parseInt(page),
      total: products.length
    });
  } catch (error) {
    console.error('Get products error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get single product
app.get('/api/products/:barcode', authenticateToken, async (req, res) => {
  try {
    const product = await findRecordByField(TABLES.PRODUCTS, 'Barcode', req.params.barcode);
    
    if (!product) {
      return res.status(404).json({ error: 'Product not found' });
    }
    
    res.json(product);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Create/Update product
app.post('/api/products', authenticateToken, async (req, res) => {
  try {
    const productData = {
      ...req.body,
      ScannedBy: [req.user.id], // Link to Users table
      ScannedByEmail: req.user.Email,
      LastModified: new Date().toISOString()
    };
    
    // Check if product exists
    const existingProduct = await findRecordByField(TABLES.PRODUCTS, 'Barcode', req.body.barcode);
    
    let product;
    if (existingProduct) {
      // Update existing product
      product = await updateRecord(TABLES.PRODUCTS, existingProduct.id, productData);
    } else {
      // Create new product
      productData.ScannedAt = new Date().toISOString();
      product = await createRecord(TABLES.PRODUCTS, {
        Barcode: productData.barcode,
        Name: productData.name,
        Brand: productData.brand || '',
        Category: productData.category || '',
        Description: productData.description || '',
        Price: parseFloat(productData.price) || 0,
        Cost: parseFloat(productData.cost) || 0,
        Weight: productData.weight || '',
        Dimensions: productData.dimensions || '',
        Quantity: parseInt(productData.quantity) || 1,
        Location: productData.location || '',
        Condition: productData.condition || 'good',
        Status: productData.status || 'pending',
        Images: productData.images || [],
        Source: productData.source || 'Manual',
        ApiData: JSON.stringify(productData.apiData || {}),
        Notes: productData.notes || '',
        ...productData
      });
    }
    
    // Log scan history
    await createRecord(TABLES.SCAN_HISTORY, {
      User: [req.user.id],
      Product: [product.id],
      Barcode: product.Barcode,
      Action: existingProduct ? 'update' : 'scan',
      Timestamp: new Date().toISOString(),
      UserEmail: req.user.Email,
      ProductName: product.Name
    });
    
    // Update user scan count
    await updateRecord(TABLES.USERS, req.user.id, {
      TotalScans: (req.user.TotalScans || 0) + 1,
      LastActive: new Date().toISOString()
    });
    
    res.json(product);
  } catch (error) {
    console.error('Create/Update product error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Upload product image
app.post('/api/products/:barcode/image', authenticateToken, upload.single('image'), async (req, res) => {
  try {
    const product = await findRecordByField(TABLES.PRODUCTS, 'Barcode', req.params.barcode);
    
    if (!product) {
      return res.status(404).json({ error: 'Product not found' });
    }
    
    const imageUrl = `${req.protocol}://${req.get('host')}/uploads/${req.file.filename}`;
    
    // Get existing images or initialize empty array
    const existingImages = product.Images || [];
    existingImages.push({ url: imageUrl });
    
    // Update product with new image
    const updatedProduct = await updateRecord(TABLES.PRODUCTS, product.id, {
      Images: existingImages
    });
    
    res.json({ imageUrl, product: updatedProduct });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Delete product
app.delete('/api/products/:barcode', authenticateToken, async (req, res) => {
  try {
    const product = await findRecordByField(TABLES.PRODUCTS, 'Barcode', req.params.barcode);
    
    if (!product) {
      return res.status(404).json({ error: 'Product not found' });
    }
    
    // Delete from Airtable
    await base(TABLES.PRODUCTS).destroy(product.id);
    
    // Log deletion
    await createRecord(TABLES.SCAN_HISTORY, {
      User: [req.user.id],
      Barcode: req.params.barcode,
      Action: 'delete',
      Timestamp: new Date().toISOString(),
      UserEmail: req.user.Email
    });
    
    res.json({ message: 'Product deleted successfully' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// ============= STATISTICS ROUTES =============

// Get statistics
app.get('/api/stats', authenticateToken, async (req, res) => {
  try {
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const todayISO = today.toISOString();
    
    // Get today's scans for this user
    const todayScansRecords = await base(TABLES.SCAN_HISTORY)
      .select({
        filterByFormula: `AND({UserEmail} = '${req.user.Email}', {Timestamp} >= '${todayISO}')`
      })
      .firstPage();
    
    // Get total products
    const allProductsRecords = await base(TABLES.PRODUCTS)
      .select({ fields: ['Barcode'] })
      .firstPage();
    
    // Get user's products
    const userProductsRecords = await base(TABLES.PRODUCTS)
      .select({
        filterByFormula: `{ScannedByEmail} = '${req.user.Email}'`,
        fields: ['Barcode']
      })
      .firstPage();
    
    // Get products by status
    const statusCounts = {};
    const statuses = ['pending', 'listed', 'sold', 'returned'];
    
    for (const status of statuses) {
      const records = await base(TABLES.PRODUCTS)
        .select({
          filterByFormula: `{Status} = '${status}'`,
          fields: ['Barcode']
        })
        .firstPage();
      statusCounts[status] = records.length;
    }
    
    // Get recent scans
    const recentScansRecords = await base(TABLES.SCAN_HISTORY)
      .select({
        filterByFormula: `{UserEmail} = '${req.user.Email}'`,
        maxRecords: 10,
        sort: [{ field: 'Timestamp', direction: 'desc' }]
      })
      .firstPage();
    
    const stats = {
      todayScans: todayScansRecords.length,
      totalProducts: allProductsRecords.length,
      userProducts: userProductsRecords.length,
      byStatus: Object.entries(statusCounts).map(([status, count]) => ({
        _id: status,
        count
      })),
      recentScans: recentScansRecords.map(cleanRecord)
    };
    
    res.json(stats);
  } catch (error) {
    console.error('Stats error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get scan history
app.get('/api/history', authenticateToken, async (req, res) => {
  try {
    const { page = 1, limit = 50 } = req.query;
    
    const records = await base(TABLES.SCAN_HISTORY)
      .select({
        filterByFormula: `{UserEmail} = '${req.user.Email}'`,
        maxRecords: parseInt(limit),
        sort: [{ field: 'Timestamp', direction: 'desc' }]
      })
      .firstPage();
    
    const history = records.map(cleanRecord);
    
    res.json({
      history,
      currentPage: parseInt(page),
      total: history.length
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// ============= EXPORT ROUTES =============

// Export to CSV
app.get('/api/export/csv', authenticateToken, async (req, res) => {
  try {
    const records = await base(TABLES.PRODUCTS)
      .select({
        sort: [{ field: 'ScannedAt', direction: 'desc' }]
      })
      .all();
    
    const products = records.map(cleanRecord);
    
    const csv = [
      ['Barcode', 'Name', 'Brand', 'Category', 'Price', 'Cost', 'Quantity', 'Location', 'Condition', 'Status', 'Scanned Date'].join(','),
      ...products.map(p => [
        p.Barcode,
        `"${p.Name || ''}"`,
        `"${p.Brand || ''}"`,
        `"${p.Category || ''}"`,
        p.Price || 0,
        p.Cost || 0,
        p.Quantity || 1,
        `"${p.Location || ''}"`,
        p.Condition || '',
        p.Status || '',
        p.ScannedAt || ''
      ].join(','))
    ].join('\n');
    
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename=inventory.csv');
    res.send(csv);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// ============= BULK OPERATIONS =============

// Bulk scan products
app.post('/api/products/bulk', authenticateToken, async (req, res) => {
  try {
    const { barcodes } = req.body;
    const results = [];
    const errors = [];
    
    for (const barcode of barcodes) {
      try {
        // Look up product
        let productData = await lookupOpenFoodFacts(barcode) ||
                        await lookupUPCItemDB(barcode) ||
                        await lookupBarcodeLookup(barcode);
        
        if (!productData) {
          productData = {
            barcode,
            name: `Product ${barcode}`,
            source: 'Manual'
          };
        }
        
        // Save to Airtable
        const product = await createRecord(TABLES.PRODUCTS, {
          Barcode: barcode,
          Name: productData.name,
          Brand: productData.brand || '',
          Category: productData.category || '',
          Description: productData.description || '',
          Price: productData.price || 0,
          Weight: productData.weight || '',
          Dimensions: productData.dimensions || '',
          Source: productData.source,
          Status: 'pending',
          Condition: 'good',
          Quantity: 1,
          ScannedBy: [req.user.id],
          ScannedByEmail: req.user.Email,
          ScannedAt: new Date().toISOString(),
          ApiData: JSON.stringify(productData)
        });
        
        results.push(product);
      } catch (error) {
        errors.push({ barcode, error: error.message });
      }
    }
    
    res.json({ 
      success: results.length,
      failed: errors.length,
      results,
      errors 
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// ============= SETTINGS ROUTES =============

// Get settings
app.get('/api/settings', authenticateToken, async (req, res) => {
  try {
    const records = await base(TABLES.SETTINGS)
      .select({
        filterByFormula: `{UserEmail} = '${req.user.Email}'`,
        maxRecords: 1
      })
      .firstPage();
    
    if (records.length > 0) {
      res.json(cleanRecord(records[0]));
    } else {
      // Return default settings
      res.json({
        apiProvider: 'openFood',
        scanSound: true,
        autoSave: false,
        defaultCondition: 'good',
        defaultStatus: 'pending'
      });
    }
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Update settings
app.post('/api/settings', authenticateToken, async (req, res) => {
  try {
    const existingSettings = await findRecordByField(TABLES.SETTINGS, 'UserEmail', req.user.Email);
    
    const settingsData = {
      ...req.body,
      User: [req.user.id],
      UserEmail: req.user.Email,
      UpdatedAt: new Date().toISOString()
    };
    
    let settings;
    if (existingSettings) {
      settings = await updateRecord(TABLES.SETTINGS, existingSettings.id, settingsData);
    } else {
      settings = await createRecord(TABLES.SETTINGS, settingsData);
    }
    
    res.json(settings);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// ============= SERVER START =============

// Create uploads directory if it doesn't exist
const fs = require('fs');
if (!fs.existsSync('uploads')) {
  fs.mkdirSync('uploads');
}

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log('Connected to Airtable');
});

// ============= ENVIRONMENT VARIABLES (.env file) =============
/*
Create a .env file with:

PORT=3000
JWT_SECRET=your-super-secret-jwt-key-change-this-in-production

# Airtable Configuration (REQUIRED)
AIRTABLE_API_KEY=keyXXXXXXXXXXXXXX
AIRTABLE_BASE_ID=appXXXXXXXXXXXXXX

# Product Lookup APIs (Optional but recommended)
UPC_ITEMDB_KEY=your-upc-itemdb-api-key
BARCODE_LOOKUP_KEY=your-barcode-lookup-api-key

# Optional: Cloudinary for image storage (instead of local)
CLOUDINARY_CLOUD_NAME=your-cloud-name
CLOUDINARY_API_KEY=your-api-key
CLOUDINARY_API_SECRET=your-api-secret
*/

// ============= PACKAGE.JSON =============
/*
{
  "name": "inventory-scanner-backend",
  "version": "2.0.0",
  "description": "Backend for liquidation inventory scanner with Airtable",
  "main": "server.js",
  "scripts": {
    "start": "node server.js",
    "dev": "nodemon server.js"
  },
  "dependencies": {
    "express": "^4.18.2",
    "cors": "^2.8.5",
    "airtable": "^0.12.2",
    "bcryptjs": "^2.4.3",
    "jsonwebtoken": "^9.0.0",
    "axios": "^1.4.0",
    "dotenv": "^16.0.3",
    "multer": "^1.4.5-lts.1",
    "cloudinary": "^1.40.0"
  },
  "devDependencies": {
    "nodemon": "^2.0.22"
  }
}
*/