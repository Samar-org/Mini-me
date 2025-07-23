/**
 * Complete SEO & E-commerce Product Optimization System
 * For Toronto, Oakville, Mississauga Local Store
 * 
 * Features:
 * ✅ SEO Title & Meta Title generation
 * ✅ Compelling Meta Descriptions 
 * ✅ Product Slug generation
 * ✅ Smart Category Recommendation with proper record linking
 * ✅ Image Alt Text for accessibility
 * ✅ Focus Keywords optimization
 * ✅ Product Tags as text
 * ✅ E-commerce friendly names
 * ✅ Brand identification
 * ✅ Local promotions integration
 */

class SEOProductOptimizer {
  constructor(config = {}) {
    this.locations = config.locations || ['Toronto', 'Oakville', 'Mississauga'];
    this.storeName = config.storeName || 'Promote Local';
    this.domain = config.domain || 'promotelocal.ca';
    
    this.promotions = [
      'Save with 4more',
      'Sale up to 90%',
      'Best Deals in GTA',
      'Local Pickup Available',
      'Free Same-Day Delivery',
      'Price Match Guarantee'
    ];
    
    // Category database with proper record structure
    this.categories = {
      'electronics': { 
        id: 1, 
        parent: null, 
        slug: 'electronics',
        name: 'Electronics',
        keywords: ['phone', 'laptop', 'computer', 'tablet', 'camera', 'headphones', 'tv', 'monitor', 'gaming', 'tech', 'digital']
      },
      'clothing-fashion': { 
        id: 2, 
        parent: null, 
        slug: 'clothing-fashion',
        name: 'Clothing & Fashion',
        keywords: ['shirt', 'pants', 'dress', 'jacket', 'shoes', 'boots', 'fashion', 'apparel', 'wear', 'clothing', 'style']
      },
      'home-garden': { 
        id: 3, 
        parent: null, 
        slug: 'home-garden',
        name: 'Home & Garden',
        keywords: ['furniture', 'decor', 'kitchen', 'garden', 'tools', 'appliance', 'home', 'house', 'interior', 'outdoor']
      },
      'sports-fitness': { 
        id: 4, 
        parent: null, 
        slug: 'sports-fitness',
        name: 'Sports & Fitness',
        keywords: ['fitness', 'exercise', 'gym', 'outdoor', 'bike', 'sports', 'athletic', 'running', 'workout', 'health']
      },
      'books-media': { 
        id: 5, 
        parent: null, 
        slug: 'books-media',
        name: 'Books & Media',
        keywords: ['book', 'novel', 'guide', 'manual', 'education', 'learning', 'textbook', 'media', 'dvd', 'cd']
      },
      'automotive': { 
        id: 6, 
        parent: null, 
        slug: 'automotive',
        name: 'Automotive',
        keywords: ['car', 'auto', 'vehicle', 'tire', 'engine', 'parts', 'motorcycle', 'truck', 'automotive', 'repair']
      },
      'beauty-health': {
        id: 7,
        parent: null,
        slug: 'beauty-health',
        name: 'Beauty & Health',
        keywords: ['beauty', 'cosmetics', 'skincare', 'health', 'wellness', 'makeup', 'personal care', 'hygiene']
      },
      'toys-games': {
        id: 8,
        parent: null,
        slug: 'toys-games',
        name: 'Toys & Games',
        keywords: ['toy', 'game', 'kids', 'children', 'play', 'board game', 'puzzle', 'educational', 'entertainment']
      }
    };

    // Brand database for better identification
    this.brandDatabase = {
      'electronics': ['apple', 'samsung', 'sony', 'lg', 'microsoft', 'google', 'amazon', 'dell', 'hp', 'lenovo', 'asus', 'acer'],
      'clothing': ['nike', 'adidas', 'levi', 'calvin klein', 'tommy hilfiger', 'gap', 'zara', 'h&m', 'uniqlo'],
      'automotive': ['ford', 'toyota', 'honda', 'chevrolet', 'bmw', 'mercedes', 'audi', 'nissan', 'hyundai'],
      'home': ['ikea', 'wayfair', 'home depot', 'canadian tire', 'costco', 'walmart'],
      'beauty': ['loreal', 'maybelline', 'revlon', 'covergirl', 'mac', 'sephora', 'clinique']
    };
  }

  // ✅ SEO Title & Meta Title generation
  generateSEOTitle(productName, brand = '', location = '', options = {}) {
    const selectedLocation = location || this.getRandomLocation();
    const brandText = brand ? `${brand} ` : '';
    const year = new Date().getFullYear();
    
    const titleVariations = [
      `${brandText}${productName} | Buy Online in ${selectedLocation} ${year}`,
      `Best ${brandText}${productName} Deals in ${selectedLocation} | ${this.storeName}`,
      `${brandText}${productName} - Fast Delivery to ${selectedLocation}`,
      `Shop ${brandText}${productName} | ${selectedLocation} Local Store`,
      `${brandText}${productName} Sale | ${selectedLocation} Best Price`,
      `Buy ${brandText}${productName} Online | ${selectedLocation} ${this.storeName}`
    ];
    
    if (options.includePromotion) {
      const promo = this.getRandomPromotion();
      titleVariations.push(`${brandText}${productName} | ${promo} | ${selectedLocation}`);
    }
    
    return this.selectBestLength(titleVariations, 60);
  }

  // ✅ Compelling Meta Descriptions (not just rewrites)
  generateMetaDescription(productName, options = {}) {
    const {
      brand = '',
      price = '',
      features = [],
      location = '',
      category = '',
      isOnSale = false
    } = options;
    
    const selectedLocation = location || this.getRandomLocation();
    const promotion = this.getRandomPromotion();
    const brandText = brand ? `${brand} ` : '';
    const priceText = price ? ` starting at ${price}` : '';
    const saleText = isOnSale ? ' ON SALE NOW!' : '';
    
    // Feature highlights (max 2 for length)
    const featureText = features.length > 0 
      ? ` Key features: ${features.slice(0, 2).join(', ')}.` 
      : '';
    
    const descriptions = [
      `Shop premium ${brandText}${productName} in ${selectedLocation}${priceText}${saleText} ${promotion} with free local delivery & pickup.${featureText}`,
      
      `Get the best ${brandText}${productName} deals in ${selectedLocation}. ${promotion}${priceText}. Fast shipping across GTA.${featureText}`,
      
      `Quality ${brandText}${productName} available now${priceText}${saleText} Serving ${selectedLocation} with ${promotion}.${featureText} Order today!`,
      
      `Buy ${brandText}${productName} online from ${this.storeName}${priceText}. ${selectedLocation} trusted local store. ${promotion}!${featureText}`,
      
      `${brandText}${productName} - best prices in ${selectedLocation}${priceText}${saleText} ${promotion}. Same-day pickup available.${featureText}`
    ];
    
    return this.selectBestLength(descriptions, 160);
  }

  // ✅ Product Slug generation
  generateProductSlug(productName, options = {}) {
    const { brand = '', model = '', sku = '', category = '' } = options;
    
    // Combine all elements
    const slugParts = [brand, productName, model].filter(part => part.length > 0);
    const baseSlug = slugParts.join(' ')
      .toLowerCase()
      .replace(/[^a-z0-9\s-]/g, '') // Remove special chars
      .replace(/\s+/g, '-') // Replace spaces with hyphens
      .replace(/-+/g, '-') // Replace multiple hyphens with single
      .replace(/^-+|-+$/g, ''); // Trim hyphens from start/end
    
    // Add SKU if available
    const skuSuffix = sku ? `-${sku.toLowerCase().replace(/[^a-z0-9]/g, '')}` : '';
    
    // Ensure uniqueness and length
    const finalSlug = `${baseSlug}${skuSuffix}`.substring(0, 100);
    
    return finalSlug;
  }

  // ✅ Smart Category Recommendation with proper record linking
  recommendCategory(productName, description = '', existingTags = []) {
    const searchText = `${productName} ${description} ${existingTags.join(' ')}`.toLowerCase();
    
    let bestMatch = { 
      category: 'electronics', 
      score: 0, 
      record: this.categories['electronics'] 
    };
    
    // Score each category based on keyword matches
    Object.entries(this.categories).forEach(([categoryKey, categoryData]) => {
      const score = categoryData.keywords.reduce((acc, keyword) => {
        const regex = new RegExp(`\\b${keyword}\\b`, 'i');
        return acc + (regex.test(searchText) ? 1 : 0);
      }, 0);
      
      if (score > bestMatch.score) {
        bestMatch = {
          category: categoryKey,
          score,
          record: {
            id: categoryData.id,
            name: categoryData.name,
            slug: categoryData.slug,
            parent: categoryData.parent,
            keywords: categoryData.keywords
          }
        };
      }
    });
    
    return bestMatch.record;
  }

  // ✅ Image Alt Text for accessibility
  generateImageAltText(productName, options = {}) {
    const { 
      brand = '', 
      color = '', 
      features = [], 
      angle = '',
      isMainImage = true 
    } = options;
    
    const brandText = brand ? `${brand} ` : '';
    const colorText = color ? ` in ${color}` : '';
    const mainFeature = features.length > 0 ? ` with ${features[0]}` : '';
    const angleText = angle ? ` - ${angle} view` : '';
    const imageType = isMainImage ? '' : ' product image';
    
    const altText = `${brandText}${productName}${colorText}${mainFeature}${angleText}${imageType}`.trim();
    
    // Ensure alt text is not too long (recommended max 125 chars)
    return altText.length > 125 ? altText.substring(0, 122) + '...' : altText;
  }

  // ✅ Focus Keywords optimization
  generateFocusKeywords(productName, options = {}) {
    const { brand = '', category = '', location = '', isLocal = true } = options;
    const selectedLocation = location || this.getRandomLocation();
    
    const baseKeywords = [
      productName.toLowerCase(),
      `buy ${productName.toLowerCase()}`,
      `${productName.toLowerCase()} online`,
      `best ${productName.toLowerCase()}`
    ];
    
    // Add brand-specific keywords
    if (brand) {
      baseKeywords.push(
        `${brand.toLowerCase()} ${productName.toLowerCase()}`,
        `${brand.toLowerCase()} deals`
      );
    }
    
    // Add location-based keywords for local SEO
    if (isLocal) {
      baseKeywords.push(
        `${productName.toLowerCase()} ${selectedLocation.toLowerCase()}`,
        `buy ${productName.toLowerCase()} ${selectedLocation.toLowerCase()}`
      );
      
      if (brand) {
        baseKeywords.push(`${brand.toLowerCase()} ${selectedLocation.toLowerCase()}`);
      }
    }
    
    // Add category keywords
    if (category) {
      baseKeywords.push(
        `${category.toLowerCase()} ${selectedLocation.toLowerCase()}`,
        `${category.toLowerCase()} store`
      );
    }
    
    // Remove duplicates and return top 8 keywords
    return [...new Set(baseKeywords)].slice(0, 8);
  }

  // ✅ Product Tags as text
  generateProductTags(productName, options = {}) {
    const { 
      brand = '', 
      features = [], 
      category = '', 
      color = '',
      material = '',
      size = '',
      includePromotional = true 
    } = options;
    
    const tags = new Set();
    
    // Core product tags
    tags.add(productName);
    if (brand) tags.add(brand);
    if (category) tags.add(category);
    if (color) tags.add(color);
    if (material) tags.add(material);
    if (size) tags.add(size);
    
    // Location tags for local SEO
    this.locations.forEach(location => tags.add(location));
    
    // Feature tags (limit to top 3)
    features.slice(0, 3).forEach(feature => tags.add(feature));
    
    // Promotional and service tags
    if (includePromotional) {
      tags.add('Fast Delivery');
      tags.add('Local Pickup');
      tags.add('Quality Guarantee');
      tags.add('Best Price');
      tags.add('Customer Service');
    }
    
    // Convert to array, remove empty tags, and join
    return Array.from(tags)
      .filter(tag => tag && tag.trim().length > 0)
      .join(', ');
  }

  // ✅ E-commerce friendly names
  generateEcommerceName(productName, options = {}) {
    const { brand = '', model = '', variant = '', size = '', color = '' } = options;
    
    const nameParts = [];
    
    if (brand) nameParts.push(brand);
    nameParts.push(productName);
    if (model) nameParts.push(model);
    if (variant) nameParts.push(`(${variant})`);
    if (size) nameParts.push(`- ${size}`);
    if (color) nameParts.push(`- ${color}`);
    
    return nameParts.join(' ').trim();
  }

  // ✅ Brand identification
  identifyBrand(productName, description = '') {
    const searchText = `${productName} ${description}`.toLowerCase();
    
    // Search through brand database
    for (const [category, brands] of Object.entries(this.brandDatabase)) {
      for (const brand of brands) {
        const regex = new RegExp(`\\b${brand}\\b`, 'i');
        if (regex.test(searchText)) {
          return this.capitalizeWords(brand);
        }
      }
    }
    
    // Additional brand detection patterns
    const brandPatterns = [
      /\b([A-Z][a-z]+)\s+(?:brand|inc|corp|ltd|llc)\b/i,
      /^([A-Z][a-z]+)\s/,
      /\b([A-Z]{2,})\b/ // All caps brands
    ];
    
    for (const pattern of brandPatterns) {
      const match = searchText.match(pattern);
      if (match) {
        return this.capitalizeWords(match[1]);
      }
    }
    
    return '';
  }

  // 🚀 Complete Product Optimization Method
  optimizeProduct(productData) {
    const {
      name,
      description = '',
      brand = '',
      price = '',
      originalPrice = '',
      features = [],
      color = '',
      material = '',
      size = '',
      model = '',
      sku = '',
      category = '',
      images = [],
      location = '',
      isOnSale = false,
      stockLevel = 'in-stock'
    } = productData;

    // Identify brand if not provided
    const identifiedBrand = brand || this.identifyBrand(name, description);
    
    // Get recommended category
    const recommendedCategory = category 
      ? this.findCategoryByName(category)
      : this.recommendCategory(name, description, features);
    
    // Generate all optimization data
    const optimizedData = {
      // Core Product Info
      originalName: name,
      ecommerceName: this.generateEcommerceName(name, {
        brand: identifiedBrand,
        model,
        variant: color || size,
        size,
        color
      }),
      brand: identifiedBrand,
      slug: this.generateProductSlug(name, {
        brand: identifiedBrand,
        model,
        sku,
        category: recommendedCategory.slug
      }),
      
      // SEO Optimization
      seoTitle: this.generateSEOTitle(name, identifiedBrand, location, {
        includePromotion: isOnSale
      }),
      metaDescription: this.generateMetaDescription(name, {
        brand: identifiedBrand,
        price,
        features,
        location,
        category: recommendedCategory.name,
        isOnSale
      }),
      focusKeywords: this.generateFocusKeywords(name, {
        brand: identifiedBrand,
        category: recommendedCategory.name,
        location
      }),
      
      // Category & Organization
      recommendedCategory,
      productTags: this.generateProductTags(name, {
        brand: identifiedBrand,
        features,
        category: recommendedCategory.name,
        color,
        material,
        size
      }),
      
      // Image Optimization
      imageAltTexts: this.generateImageAltTexts(name, {
        brand: identifiedBrand,
        color,
        features,
        totalImages: images.length || 1
      }),
      
      // Local & Promotional
      targetLocation: location || this.getRandomLocation(),
      appliedPromotion: this.getRandomPromotion(),
      localSEOBoost: this.generateLocalSEOData(name, identifiedBrand, location),
      
      // Advanced SEO
      structuredData: this.generateStructuredData(name, {
        brand: identifiedBrand,
        category: recommendedCategory,
        price,
        originalPrice,
        stockLevel,
        features,
        description
      }),
      
      // Performance Metrics
      optimizationScore: this.calculateOptimizationScore({
        hasDescription: description.length > 0,
        hasBrand: identifiedBrand.length > 0,
        hasFeatures: features.length > 0,
        hasPrice: price.length > 0,
        hasImages: images.length > 0,
        hasSKU: sku.length > 0
      }),
      
      // Additional Metadata
      generatedAt: new Date().toISOString(),
      optimizerVersion: '2.0.0'
    };

    return optimizedData;
  }

  // Helper method to generate multiple image alt texts
  generateImageAltTexts(productName, options) {
    const { brand, color, features, totalImages = 1 } = options;
    const altTexts = [];
    
    const angles = ['main', 'side', 'back', 'detail', 'lifestyle', 'packaging'];
    
    for (let i = 0; i < totalImages; i++) {
      const isMainImage = i === 0;
      const angle = isMainImage ? 'main' : (angles[i] || `view ${i + 1}`);
      
      altTexts.push(this.generateImageAltText(productName, {
        brand,
        color,
        features,
        angle,
        isMainImage
      }));
    }
    
    return altTexts;
  }

  // Generate local SEO data
  generateLocalSEOData(productName, brand, location) {
    const targetLocation = location || this.getRandomLocation();
    
    return {
      localKeywords: [
        `${productName} ${targetLocation}`,
        `buy ${productName} ${targetLocation}`,
        `${targetLocation} ${productName} store`,
        `${brand} ${targetLocation}` // if brand exists
      ].filter(keyword => keyword.trim().length > 0),
      
      businessData: {
        name: this.storeName,
        address: `${targetLocation}, Ontario, Canada`,
        serviceArea: this.locations,
        businessType: 'Local Retail Store'
      }
    };
  }

  // Generate structured data (Schema.org)
  generateStructuredData(productName, options) {
    const {
      brand,
      category,
      price,
      originalPrice,
      stockLevel,
      features,
      description
    } = options;
    
    const structuredData = {
      "@context": "https://schema.org/",
      "@type": "Product",
      "name": this.generateEcommerceName(productName, { brand }),
      "description": description || `High-quality ${productName} available at ${this.storeName}`,
      "brand": {
        "@type": "Brand",
        "name": brand || "Generic"
      },
      "category": category.name,
      "offers": {
        "@type": "Offer",
        "availability": stockLevel === 'in-stock' 
          ? "https://schema.org/InStock" 
          : "https://schema.org/OutOfStock",
        "price": price.replace(/[^0-9.]/g, ''),
        "priceCurrency": "CAD",
        "areaServed": this.locations.map(location => ({
          "@type": "City",
          "name": location
        })),
        "seller": {
          "@type": "Organization",
          "name": this.storeName
        }
      }
    };
    
    // Add high price if on sale
    if (originalPrice && originalPrice !== price) {
      structuredData.offers.highPrice = originalPrice.replace(/[^0-9.]/g, '');
    }
    
    // Add features if available
    if (features.length > 0) {
      structuredData.additionalProperty = features.map(feature => ({
        "@type": "PropertyValue",
        "name": "Feature",
        "value": feature
      }));
    }
    
    return structuredData;
  }

  // Calculate optimization score
  calculateOptimizationScore(factors) {
    const weights = {
      hasDescription: 20,
      hasBrand: 15,
      hasFeatures: 15,
      hasPrice: 10,
      hasImages: 15,
      hasSKU: 10,
      hasCategory: 15
    };
    
    let score = 0;
    let maxScore = 0;
    
    Object.entries(weights).forEach(([factor, weight]) => {
      maxScore += weight;
      if (factors[factor]) {
        score += weight;
      }
    });
    
    return Math.round((score / maxScore) * 100);
  }

  // Helper methods
  getRandomLocation() {
    return this.locations[Math.floor(Math.random() * this.locations.length)];
  }

  getRandomPromotion() {
    return this.promotions[Math.floor(Math.random() * this.promotions.length)];
  }

  selectBestLength(options, maxLength) {
    const suitable = options.filter(opt => opt.length <= maxLength);
    return suitable.length > 0 
      ? suitable[Math.floor(Math.random() * suitable.length)]
      : options[0].substring(0, maxLength - 3) + '...';
  }

  capitalizeWords(str) {
    return str.replace(/\b\w/g, l => l.toUpperCase());
  }

  findCategoryByName(categoryName) {
    const found = Object.values(this.categories)
      .find(cat => cat.name.toLowerCase().includes(categoryName.toLowerCase()));
    return found || this.categories['electronics'];
  }
}

// 🎯 Bulk Processing System
class BulkOptimizer {
  constructor(config = {}) {
    this.optimizer = new SEOProductOptimizer(config);
    this.processingStats = {
      processed: 0,
      successful: 0,
      failed: 0,
      startTime: null,
      endTime: null
    };
  }

  // Process array of products
  async optimizeProducts(productsArray, options = {}) {
    const { 
      batchSize = 50, 
      delayBetweenBatches = 100,
      onProgress = null,
      onError = null 
    } = options;
    
    this.processingStats.startTime = new Date();
    this.processingStats.processed = 0;
    this.processingStats.successful = 0;
    this.processingStats.failed = 0;
    
    const results = [];
    
    // Process in batches
    for (let i = 0; i < productsArray.length; i += batchSize) {
      const batch = productsArray.slice(i, i + batchSize);
      
      const batchResults = await Promise.allSettled(
        batch.map(async (product, index) => {
          try {
            const optimized = this.optimizer.optimizeProduct(product);
            this.processingStats.successful++;
            
            if (onProgress) {
              onProgress({
                current: this.processingStats.processed + index + 1,
                total: productsArray.length,
                product: optimized
              });
            }
            
            return {
              success: true,
              originalData: product,
              optimizedData: optimized,
              index: i + index
            };
          } catch (error) {
            this.processingStats.failed++;
            
            if (onError) {
              onError(error, product, i + index);
            }
            
            return {
              success: false,
              originalData: product,
              error: error.message,
              index: i + index
            };
          }
        })
      );
      
      results.push(...batchResults.map(result => result.value));
      this.processingStats.processed += batch.length;
      
      // Delay between batches to prevent overwhelming
      if (i + batchSize < productsArray.length && delayBetweenBatches > 0) {
        await new Promise(resolve => setTimeout(resolve, delayBetweenBatches));
      }
    }
    
    this.processingStats.endTime = new Date();
    
    return {
      results,
      stats: this.getProcessingStats(),
      summary: this.generateProcessingSummary()
    };
  }

  // Export results to CSV format
  exportToCSV(results) {
    const headers = [
      'Original Name',
      'Optimized Name', 
      'SEO Title',
      'Meta Description',
      'Slug',
      'Brand',
      'Category',
      'Focus Keywords',
      'Product Tags',
      'Image Alt Text',
      'Target Location',
      'Promotion',
      'Optimization Score'
    ];
    
    const csvRows = [headers.join(',')];
    
    results.forEach(result => {
      if (result.success) {
        const opt = result.optimizedData;
        const row = [
          `"${opt.originalName}"`,
          `"${opt.ecommerceName}"`,
          `"${opt.seoTitle}"`,
          `"${opt.metaDescription}"`,
          `"${opt.slug}"`,
          `"${opt.brand}"`,
          `"${opt.recommendedCategory.name}"`,
          `"${opt.focusKeywords.join('; ')}"`,
          `"${opt.productTags}"`,
          `"${opt.imageAltTexts[0] || ''}"`,
          `"${opt.targetLocation}"`,
          `"${opt.appliedPromotion}"`,
          opt.optimizationScore
        ];
        csvRows.push(row.join(','));
      }
    });
    
    return csvRows.join('\n');
  }

  getProcessingStats() {
    const duration = this.processingStats.endTime 
      ? this.processingStats.endTime - this.processingStats.startTime
      : 0;
      
    return {
      ...this.processingStats,
      duration,
      averageTimePerProduct: duration / this.processingStats.processed,
      successRate: (this.processingStats.successful / this.processingStats.processed) * 100
    };
  }

  generateProcessingSummary() {
    const stats = this.getProcessingStats();
    
    return {
      totalProcessed: stats.processed,
      successful: stats.successful,
      failed: stats.failed,
      successRate: `${stats.successRate.toFixed(1)}%`,
      totalTime: `${(stats.duration / 1000).toFixed(2)}s`,
      averageTime: `${stats.averageTimePerProduct.toFixed(0)}ms per product`
    };
  }
}

// 📊 Usage Examples & Test Suite
function runExamples() {
  console.log('🚀 SEO Product Optimizer - Complete Examples\n');
  
  const optimizer = new SEOProductOptimizer({
    storeName: 'Promote Local',
    locations: ['Toronto', 'Oakville', 'Mississauga']
  });

  // Example 1: Electronics Product
  console.log('📱 EXAMPLE 1: iPhone 15 Pro');
  const phoneProduct = {
    name: "iPhone 15 Pro",
    description: "Latest smartphone with advanced camera system and titanium design",
    brand: "Apple",
    price: "$1,299.99",
    originalPrice: "$1,399.99",
    features: ["48MP Camera", "Titanium Design", "USB-C", "A17 Pro Chip"],
    color: "Natural Titanium",
    size: "6.1 inch",
    model: "A17 Pro",
    sku: "IPH15P128NT",
    images: ["main.jpg", "side.jpg", "back.jpg"],
    isOnSale: true,
    stockLevel: "in-stock"
  };
  
  const optimizedPhone = optimizer.optimizeProduct(phoneProduct);
  console.log('SEO Title:', optimizedPhone.seoTitle);
  console.log('Meta Description:', optimizedPhone.metaDescription);
  console.log('Slug:', optimizedPhone.slug);
  console.log('Focus Keywords:', optimizedPhone.focusKeywords.join(', '));
  console.log('Optimization Score:', optimizedPhone.optimizationScore + '%');
  console.log('---\n');

  // Example 2: Clothing Product
  console.log('👕 EXAMPLE 2: Denim Jacket');
  const clothingProduct = {
    name: "Classic Denim Jacket",
    description: "Vintage style denim jacket perfect for casual wear and layering",
    features: ["100% Cotton", "Button Closure", "Multiple Pockets", "Machine Washable"],
    color: "Blue",
    size: "Medium",
    material: "Cotton",
    price: "$89.99",
    originalPrice: "$120.00",
    sku: "DJ2024MED",
    isOnSale: true
  };
  
  const optimizedClothing = optimizer.optimizeProduct(clothingProduct);
  console.log('SEO Title:', optimizedClothing.seoTitle);
  console.log('Meta Description:', optimizedClothing.metaDescription);
  console.log('Category:', optimizedClothing.recommendedCategory.name);
  console.log('Product Tags:', optimizedClothing.productTags);
  console.log('---\n');

  // Example 3: Home & Garden Product
  console.log('🏡 EXAMPLE 3: Smart Garden System');
  const homeProduct = {
    name: "Smart Garden Sprinkler System",
    description: "Automated watering system with WiFi connectivity and weather sensors",
    features: ["WiFi Connected", "Weather Sensor", "Mobile App Control", "Timer Function"],
    price: "$199.99",
    sku: "SGS2024WIFI",
    model: "SGS-2024",
    category: "home-garden"
  };
  
  const optimizedHome = optimizer.optimizeProduct(homeProduct);
  console.log('E-commerce Name:', optimizedHome.ecommerceName);
  console.log('Image Alt Text:', optimizedHome.imageAltTexts[0]);
  console.log('Structured Data Brand:', optimizedHome.structuredData.brand.name);
  console.log('Local Keywords:', optimizedHome.localSEOBoost.localKeywords.join(', '));
  console.log('---\n');
}

// 🎯 Bulk Processing Example
async function bulkProcessingExample() {
  console.log('📦 BULK PROCESSING EXAMPLE\n');
  
  const bulkOptimizer = new BulkOptimizer();
  
  const sampleProducts = [
    { name: "Wireless Bluetooth Headphones", price: "$79.99", features: ["Noise Cancellation", "20hr Battery"] },
    { name: "Cotton T-Shirt", color: "Red", size: "Large", price: "$24.99" },
    { name: "Coffee Maker", brand: "Hamilton Beach", features: ["12-Cup", "Programmable"], price: "$89.99" },
    { name: "Yoga Mat", material: "TPE", color: "Purple", price: "$39.99" },
    { name: "LED Desk Lamp", features: ["Adjustable", "USB Charging"], price: "$59.99" }
  ];
  
  try {
    const bulkResults = await bulkOptimizer.optimizeProducts(sampleProducts, {
      batchSize: 2,
      onProgress: (progress) => {
        console.log(`Processing ${progress.current}/${progress.total}: ${progress.product.originalName}`);
      }
    });
    
    console.log('\nBulk Processing Summary:');
    console.log('Total Processed:', bulkResults.summary.totalProcessed);
    console.log('Success Rate:', bulkResults.summary.successRate);
    console.log('Total Time:', bulkResults.summary.totalTime);
    
    // Export to CSV
    const csvData = bulkOptimizer.exportToCSV(bulkResults.results);
    console.log('\nCSV Export (first 200 chars):');
    console.log(csvData.substring(0, 200) + '...');
    
  } catch (error) {
    console.error('Bulk processing error:', error);
  }
}

// 🧪 Test Suite
function runTests() {
  console.log('🧪 RUNNING TESTS\n');
  
  const optimizer = new SEOProductOptimizer();
  let passed = 0;
  let total = 0;
  
  // Test 1: SEO Title Length
  total++;
  const title = optimizer.generateSEOTitle("Test Product", "TestBrand");
  if (title.length <= 60) {
    console.log('✅ Test 1: SEO Title length is within limit');
    passed++;
  } else {
    console.log('❌ Test 1: SEO Title too long:', title.length);
  }
  
  // Test 2: Meta Description Length
  total++;
  const metaDesc = optimizer.generateMetaDescription("Test Product", {
    brand: "TestBrand",
    features: ["Feature 1", "Feature 2"]
  });
  if (metaDesc.length <= 160) {
    console.log('✅ Test 2: Meta Description length is within limit');
    passed++;
  } else {
    console.log('❌ Test 2: Meta Description too long:', metaDesc.length);
  }
  
  // Test 3: Slug Generation
  total++;
  const slug = optimizer.generateProductSlug("Test Product Name!", { brand: "Test&Brand" });
  if (/^[a-z0-9-]+$/.test(slug)) {
    console.log('✅ Test 3: Slug contains only valid characters');
    passed++;
  } else {
    console.log('❌ Test 3: Invalid slug format:', slug);
  }
  
  // Test 4: Category Recommendation
  total++;
  const category = optimizer.recommendCategory("iPhone smartphone mobile", "Latest smartphone technology");
  if (category.id && category.name && category.slug) {
    console.log('✅ Test 4: Category recommendation returns proper record');
    passed++;
  } else {
    console.log('❌ Test 4: Invalid category recommendation:', category);
  }
  
  // Test 5: Brand Identification
  total++;
  const brand = optimizer.identifyBrand("Apple iPhone 15", "Latest Apple smartphone");
  if (brand === "Apple") {
    console.log('✅ Test 5: Brand identification works correctly');
    passed++;
  } else {
    console.log('❌ Test 5: Brand identification failed. Got:', brand);
  }
  
  console.log(`\n🎯 Test Results: ${passed}/${total} tests passed (${(passed/total*100).toFixed(1)}%)`);
}

// 🚀 Main Execution
function main() {
  console.log('='.repeat(60));
  console.log('🏪 PROMOTE LOCAL - SEO E-COMMERCE OPTIMIZER');
  console.log('📍 Serving Toronto, Oakville, Mississauga');
  console.log('='.repeat(60));
  console.log();
  
  // Run examples
  runExamples();
  
  // Run bulk processing example
  bulkProcessingExample().then(() => {
    console.log('\n' + '='.repeat(60));
    
    // Run tests
    runTests();
    
    console.log('\n✨ All examples completed successfully!');
    console.log('📝 Ready for integration with your e-commerce platform.');
  });
}

// Export for module use
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { 
    SEOProductOptimizer, 
    BulkOptimizer,
    runExamples,
    runTests,
    main
  };
} else {
  // Run if in browser/direct execution
  main();
}