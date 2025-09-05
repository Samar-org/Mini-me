// App.js - Main React Native Application
import React, { useState, useEffect } from 'react';
import {
  StyleSheet,
  Text,
  View,
  TextInput,
  TouchableOpacity,
  ScrollView,
  Alert,
  ActivityIndicator,
  Image,
  RefreshControl,
  Modal,
  FlatList,
  StatusBar,
  SafeAreaView,
  KeyboardAvoidingView,
  Platform,
  Dimensions
} from 'react-native';
import AsyncStorage from '@react-native-async-storage/async-storage';
import { Camera } from 'expo-camera';
import { BarCodeScanner } from 'expo-barcode-scanner';
import * as ImagePicker from 'expo-image-picker';
import { LinearGradient } from 'expo-linear-gradient';
import { Ionicons, MaterialIcons, Feather } from '@expo/vector-icons';
import axios from 'axios';

const { width, height } = Dimensions.get('window');
const API_URL = 'http://your-server-ip:3000/api'; // Change to your server URL

// ============= AUTH CONTEXT =============
const AuthContext = React.createContext();

// ============= MAIN APP COMPONENT =============
export default function App() {
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    checkAuthStatus();
  }, []);

  const checkAuthStatus = async () => {
    try {
      const token = await AsyncStorage.getItem('authToken');
      const userData = await AsyncStorage.getItem('userData');
      
      if (token && userData) {
        setUser(JSON.parse(userData));
        axios.defaults.headers.common['Authorization'] = `Bearer ${token}`;
      }
    } catch (error) {
      console.error('Auth check error:', error);
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <View style={styles.loadingContainer}>
        <ActivityIndicator size="large" color="#667eea" />
      </View>
    );
  }

  return (
    <AuthContext.Provider value={{ user, setUser }}>
      <StatusBar barStyle="light-content" backgroundColor="#667eea" />
      {user ? <MainApp /> : <LoginScreen />}
    </AuthContext.Provider>
  );
}

// ============= LOGIN SCREEN =============
function LoginScreen() {
  const { setUser } = React.useContext(AuthContext);
  const [isLogin, setIsLogin] = useState(true);
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [name, setName] = useState('');
  const [loading, setLoading] = useState(false);

  const handleAuth = async () => {
    if (!email || !password || (!isLogin && !name)) {
      Alert.alert('Error', 'Please fill all fields');
      return;
    }

    setLoading(true);
    try {
      const endpoint = isLogin ? '/auth/login' : '/auth/register';
      const payload = isLogin ? { email, password } : { email, password, name };
      
      const response = await axios.post(`${API_URL}${endpoint}`, payload);
      
      if (response.data.token) {
        await AsyncStorage.setItem('authToken', response.data.token);
        await AsyncStorage.setItem('userData', JSON.stringify(response.data.user));
        axios.defaults.headers.common['Authorization'] = `Bearer ${response.data.token}`;
        setUser(response.data.user);
      }
    } catch (error) {
      Alert.alert('Error', error.response?.data?.error || 'Authentication failed');
    } finally {
      setLoading(false);
    }
  };

  return (
    <LinearGradient colors={['#667eea', '#764ba2']} style={styles.container}>
      <SafeAreaView style={styles.safeArea}>
        <KeyboardAvoidingView 
          behavior={Platform.OS === 'ios' ? 'padding' : 'height'}
          style={styles.keyboardView}
        >
          <ScrollView 
            contentContainerStyle={styles.scrollContent}
            keyboardShouldPersistTaps="handled"
          >
            <View style={styles.loginContainer}>
              <View style={styles.logoContainer}>
                <Ionicons name="scan-circle" size={80} color="white" />
                <Text style={styles.appTitle}>Inventory Scanner</Text>
                <Text style={styles.appSubtitle}>Liquidation Processing System</Text>
              </View>

              <View style={styles.formContainer}>
                {!isLogin && (
                  <View style={styles.inputGroup}>
                    <Ionicons name="person-outline" size={20} color="#667eea" style={styles.inputIcon} />
                    <TextInput
                      style={styles.input}
                      placeholder="Full Name"
                      value={name}
                      onChangeText={setName}
                      autoCapitalize="words"
                    />
                  </View>
                )}

                <View style={styles.inputGroup}>
                  <Ionicons name="mail-outline" size={20} color="#667eea" style={styles.inputIcon} />
                  <TextInput
                    style={styles.input}
                    placeholder="Email"
                    value={email}
                    onChangeText={setEmail}
                    keyboardType="email-address"
                    autoCapitalize="none"
                  />
                </View>

                <View style={styles.inputGroup}>
                  <Ionicons name="lock-closed-outline" size={20} color="#667eea" style={styles.inputIcon} />
                  <TextInput
                    style={styles.input}
                    placeholder="Password"
                    value={password}
                    onChangeText={setPassword}
                    secureTextEntry
                  />
                </View>

                <TouchableOpacity 
                  style={styles.authButton}
                  onPress={handleAuth}
                  disabled={loading}
                >
                  {loading ? (
                    <ActivityIndicator color="white" />
                  ) : (
                    <Text style={styles.authButtonText}>
                      {isLogin ? 'Login' : 'Sign Up'}
                    </Text>
                  )}
                </TouchableOpacity>

                <TouchableOpacity onPress={() => setIsLogin(!isLogin)}>
                  <Text style={styles.switchText}>
                    {isLogin ? "Don't have an account? Sign Up" : "Already have an account? Login"}
                  </Text>
                </TouchableOpacity>
              </View>
            </View>
          </ScrollView>
        </KeyboardAvoidingView>
      </SafeAreaView>
    </LinearGradient>
  );
}

// ============= MAIN APP =============
function MainApp() {
  const { user, setUser } = React.useContext(AuthContext);
  const [selectedTab, setSelectedTab] = useState('scanner');
  const [stats, setStats] = useState({
    todayScans: 0,
    totalProducts: 0,
    userProducts: 0
  });

  useEffect(() => {
    loadStats();
  }, []);

  const loadStats = async () => {
    try {
      const response = await axios.get(`${API_URL}/stats`);
      setStats(response.data);
    } catch (error) {
      console.error('Stats error:', error);
    }
  };

  const handleLogout = async () => {
    Alert.alert(
      'Logout',
      'Are you sure you want to logout?',
      [
        { text: 'Cancel', style: 'cancel' },
        {
          text: 'Logout',
          style: 'destructive',
          onPress: async () => {
            await AsyncStorage.removeItem('authToken');
            await AsyncStorage.removeItem('userData');
            delete axios.defaults.headers.common['Authorization'];
            setUser(null);
          }
        }
      ]
    );
  };

  return (
    <SafeAreaView style={styles.container}>
      {/* Header */}
      <LinearGradient colors={['#667eea', '#764ba2']} style={styles.header}>
        <View style={styles.headerContent}>
          <Text style={styles.headerTitle}>Hi, {user.name}!</Text>
          <TouchableOpacity onPress={handleLogout}>
            <Ionicons name="log-out-outline" size={24} color="white" />
          </TouchableOpacity>
        </View>
        
        {/* Stats Cards */}
        <ScrollView 
          horizontal 
          showsHorizontalScrollIndicator={false}
          style={styles.statsContainer}
        >
          <View style={styles.statCard}>
            <Text style={styles.statValue}>{stats.todayScans}</Text>
            <Text style={styles.statLabel}>Today's Scans</Text>
          </View>
          <View style={styles.statCard}>
            <Text style={styles.statValue}>{stats.userProducts}</Text>
            <Text style={styles.statLabel}>Your Products</Text>
          </View>
          <View style={styles.statCard}>
            <Text style={styles.statValue}>{stats.totalProducts}</Text>
            <Text style={styles.statLabel}>Total Products</Text>
          </View>
        </ScrollView>
      </LinearGradient>

      {/* Content */}
      <View style={styles.content}>
        {selectedTab === 'scanner' && <ScannerTab onStatsUpdate={loadStats} />}
        {selectedTab === 'products' && <ProductsTab />}
        {selectedTab === 'history' && <HistoryTab />}
        {selectedTab === 'settings' && <SettingsTab />}
      </View>

      {/* Bottom Navigation */}
      <View style={styles.bottomNav}>
        <TouchableOpacity 
          style={styles.navItem}
          onPress={() => setSelectedTab('scanner')}
        >
          <Ionicons 
            name="scan" 
            size={24} 
            color={selectedTab === 'scanner' ? '#667eea' : '#999'} 
          />
          <Text style={[styles.navText, selectedTab === 'scanner' && styles.navTextActive]}>
            Scanner
          </Text>
        </TouchableOpacity>

        <TouchableOpacity 
          style={styles.navItem}
          onPress={() => setSelectedTab('products')}
        >
          <Ionicons 
            name="cube-outline" 
            size={24} 
            color={selectedTab === 'products' ? '#667eea' : '#999'} 
          />
          <Text style={[styles.navText, selectedTab === 'products' && styles.navTextActive]}>
            Products
          </Text>
        </TouchableOpacity>

        <TouchableOpacity 
          style={styles.navItem}
          onPress={() => setSelectedTab('history')}
        >
          <Ionicons 
            name="time-outline" 
            size={24} 
            color={selectedTab === 'history' ? '#667eea' : '#999'} 
          />
          <Text style={[styles.navText, selectedTab === 'history' && styles.navTextActive]}>
            History
          </Text>
        </TouchableOpacity>

        <TouchableOpacity 
          style={styles.navItem}
          onPress={() => setSelectedTab('settings')}
        >
          <Ionicons 
            name="settings-outline" 
            size={24} 
            color={selectedTab === 'settings' ? '#667eea' : '#999'} 
          />
          <Text style={[styles.navText, selectedTab === 'settings' && styles.navTextActive]}>
            Settings
          </Text>
        </TouchableOpacity>
      </View>
    </SafeAreaView>
  );
}

// ============= SCANNER TAB =============
function ScannerTab({ onStatsUpdate }) {
  const [hasPermission, setHasPermission] = useState(null);
  const [scanning, setScanning] = useState(false);
  const [manualBarcode, setManualBarcode] = useState('');
  const [loading, setLoading] = useState(false);
  const [product, setProduct] = useState(null);
  const [showProductModal, setShowProductModal] = useState(false);

  useEffect(() => {
    requestCameraPermission();
  }, []);

  const requestCameraPermission = async () => {
    const { status } = await Camera.requestCameraPermissionsAsync();
    setHasPermission(status === 'granted');
  };

  const handleBarCodeScanned = ({ type, data }) => {
    setScanning(false);
    lookupProduct(data);
  };

  const lookupProduct = async (barcode) => {
    setLoading(true);
    try {
      // First check if product exists in database
      const existingProduct = await axios.get(`${API_URL}/products/${barcode}`);
      
      if (existingProduct.data) {
        setProduct(existingProduct.data);
        setShowProductModal(true);
      }
    } catch (error) {
      // Product not in database, lookup from APIs
      try {
        const lookupResponse = await axios.post(`${API_URL}/products/lookup`, {
          barcode,
          provider: 'all'
        });
        
        if (lookupResponse.data.success) {
          setProduct(lookupResponse.data.data);
          setShowProductModal(true);
        } else {
          // Product not found, show manual entry
          Alert.alert(
            'Product Not Found',
            'This product was not found in any database. Would you like to add it manually?',
            [
              { text: 'Cancel', style: 'cancel' },
              { text: 'Add Manually', onPress: () => showManualEntry(barcode) }
            ]
          );
        }
      } catch (lookupError) {
        Alert.alert('Error', 'Failed to lookup product');
      }
    } finally {
      setLoading(false);
    }
  };

  const showManualEntry = (barcode) => {
    setProduct({ barcode, isManual: true });
    setShowProductModal(true);
  };

  const saveProduct = async (productData) => {
    try {
      await axios.post(`${API_URL}/products`, productData);
      Alert.alert('Success', 'Product saved successfully!');
      setShowProductModal(false);
      setProduct(null);
      onStatsUpdate();
    } catch (error) {
      Alert.alert('Error', 'Failed to save product');
    }
  };

  if (scanning) {
    return (
      <View style={styles.scannerContainer}>
        <BarCodeScanner
          onBarCodeScanned={handleBarCodeScanned}
          style={StyleSheet.absoluteFillObject}
        />
        <View style={styles.scannerOverlay}>
          <View style={styles.scannerFrame} />
          <TouchableOpacity 
            style={styles.cancelScanButton}
            onPress={() => setScanning(false)}
          >
            <Text style={styles.cancelScanText}>Cancel</Text>
          </TouchableOpacity>
        </View>
      </View>
    );
  }

  return (
    <ScrollView style={styles.tabContent}>
      {/* Scanner Button */}
      <TouchableOpacity 
        style={styles.scanButton}
        onPress={() => {
          if (hasPermission) {
            setScanning(true);
          } else {
            requestCameraPermission();
          }
        }}
        disabled={loading}
      >
        <LinearGradient
          colors={['#00d2ff', '#3a47d5']}
          style={styles.scanButtonGradient}
        >
          <Ionicons name="camera" size={32} color="white" />
          <Text style={styles.scanButtonText}>Start Camera Scanner</Text>
        </LinearGradient>
      </TouchableOpacity>

      {/* Manual Entry */}
      <View style={styles.manualEntry}>
        <TextInput
          style={styles.manualInput}
          placeholder="Enter barcode manually"
          value={manualBarcode}
          onChangeText={setManualBarcode}
          keyboardType="numeric"
        />
        <TouchableOpacity 
          style={styles.lookupButton}
          onPress={() => {
            if (manualBarcode) {
              lookupProduct(manualBarcode);
            }
          }}
          disabled={loading}
        >
          {loading ? (
            <ActivityIndicator color="white" />
          ) : (
            <Text style={styles.lookupButtonText}>Lookup</Text>
          )}
        </TouchableOpacity>
      </View>

      {/* Product Modal */}
      <Modal
        visible={showProductModal}
        animationType="slide"
        transparent={true}
      >
        <ProductModal 
          product={product}
          onSave={saveProduct}
          onClose={() => {
            setShowProductModal(false);
            setProduct(null);
          }}
        />
      </Modal>
    </ScrollView>
  );
}

// ============= PRODUCT MODAL =============
function ProductModal({ product, onSave, onClose }) {
  const [formData, setFormData] = useState({
    barcode: product?.barcode || '',
    name: product?.name || '',
    brand: product?.brand || '',
    category: product?.category || '',
    description: product?.description || '',
    price: product?.price?.toString() || '',
    cost: '',
    quantity: '1',
    location: '',
    condition: 'good',
    notes: ''
  });

  const conditions = ['new', 'like-new', 'good', 'fair', 'poor'];

  const handleSave = () => {
    if (!formData.name) {
      Alert.alert('Error', 'Product name is required');
      return;
    }

    onSave({
      ...formData,
      price: parseFloat(formData.price) || 0,
      cost: parseFloat(formData.cost) || 0,
      quantity: parseInt(formData.quantity) || 1,
      apiData: product?.isManual ? null : product
    });
  };

  return (
    <View style={styles.modalContainer}>
      <View style={styles.modalContent}>
        <View style={styles.modalHeader}>
          <Text style={styles.modalTitle}>
            {product?.isManual ? 'Add Product Manually' : 'Product Details'}
          </Text>
          <TouchableOpacity onPress={onClose}>
            <Ionicons name="close" size={24} color="#333" />
          </TouchableOpacity>
        </View>

        <ScrollView style={styles.modalBody}>
          {/* Product Images */}
          {product?.images && product.images.length > 0 && (
            <ScrollView horizontal style={styles.imageScroll}>
              {product.images.map((image, index) => (
                <Image key={index} source={{ uri: image }} style={styles.productImage} />
              ))}
            </ScrollView>
          )}

          {/* Form Fields */}
          <View style={styles.formField}>
            <Text style={styles.fieldLabel}>Barcode</Text>
            <TextInput
              style={[styles.fieldInput, styles.disabledInput]}
              value={formData.barcode}
              editable={false}
            />
          </View>

          <View style={styles.formField}>
            <Text style={styles.fieldLabel}>Product Name *</Text>
            <TextInput
              style={styles.fieldInput}
              value={formData.name}
              onChangeText={(text) => setFormData({ ...formData, name: text })}
              placeholder="Enter product name"
            />
          </View>

          <View style={styles.formRow}>
            <View style={[styles.formField, { flex: 1 }]}>
              <Text style={styles.fieldLabel}>Brand</Text>
              <TextInput
                style={styles.fieldInput}
                value={formData.brand}
                onChangeText={(text) => setFormData({ ...formData, brand: text })}
                placeholder="Brand name"
              />
            </View>

            <View style={[styles.formField, { flex: 1, marginLeft: 10 }]}>
              <Text style={styles.fieldLabel}>Category</Text>
              <TextInput
                style={styles.fieldInput}
                value={formData.category}
                onChangeText={(text) => setFormData({ ...formData, category: text })}
                placeholder="Category"
              />
            </View>
          </View>

          <View style={styles.formField}>
            <Text style={styles.fieldLabel}>Description</Text>
            <TextInput
              style={[styles.fieldInput, styles.textArea]}
              value={formData.description}
              onChangeText={(text) => setFormData({ ...formData, description: text })}
              placeholder="Product description"
              multiline
              numberOfLines={3}
            />
          </View>

          <View style={styles.formRow}>
            <View style={[styles.formField, { flex: 1 }]}>
              <Text style={styles.fieldLabel}>Selling Price</Text>
              <TextInput
                style={styles.fieldInput}
                value={formData.price}
                onChangeText={(text) => setFormData({ ...formData, price: text })}
                placeholder="0.00"
                keyboardType="decimal-pad"
              />
            </View>

            <View style={[styles.formField, { flex: 1, marginLeft: 10 }]}>
              <Text style={styles.fieldLabel}>Cost</Text>
              <TextInput
                style={styles.fieldInput}
                value={formData.cost}
                onChangeText={(text) => setFormData({ ...formData, cost: text })}
                placeholder="0.00"
                keyboardType="decimal-pad"
              />
            </View>
          </View>

          <View style={styles.formRow}>
            <View style={[styles.formField, { flex: 1 }]}>
              <Text style={styles.fieldLabel}>Quantity</Text>
              <TextInput
                style={styles.fieldInput}
                value={formData.quantity}
                onChangeText={(text) => setFormData({ ...formData, quantity: text })}
                placeholder="1"
                keyboardType="number-pad"
              />
            </View>

            <View style={[styles.formField, { flex: 1, marginLeft: 10 }]}>
              <Text style={styles.fieldLabel}>Location</Text>
              <TextInput
                style={styles.fieldInput}
                value={formData.location}
                onChangeText={(text) => setFormData({ ...formData, location: text })}
                placeholder="A1-B2"
              />
            </View>
          </View>

          <View style={styles.formField}>
            <Text style={styles.fieldLabel}>Condition</Text>
            <ScrollView horizontal showsHorizontalScrollIndicator={false}>
              {conditions.map((condition) => (
                <TouchableOpacity
                  key={condition}
                  style={[
                    styles.conditionChip,
                    formData.condition === condition && styles.conditionChipActive
                  ]}
                  onPress={() => setFormData({ ...formData, condition })}
                >
                  <Text style={[
                    styles.conditionChipText,
                    formData.condition === condition && styles.conditionChipTextActive
                  ]}>
                    {condition}
                  </Text>
                </TouchableOpacity>
              ))}
            </ScrollView>
          </View>

          <View style={styles.formField}>
            <Text style={styles.fieldLabel}>Notes</Text>
            <TextInput
              style={[styles.fieldInput, styles.textArea]}
              value={formData.notes}
              onChangeText={(text) => setFormData({ ...formData, notes: text })}
              placeholder="Additional notes"
              multiline
              numberOfLines={3}
            />
          </View>
        </ScrollView>

        <View style={styles.modalFooter}>
          <TouchableOpacity style={styles.cancelButton} onPress={onClose}>
            <Text style={styles.cancelButtonText}>Cancel</Text>
          </TouchableOpacity>
          <TouchableOpacity style={styles.saveButton} onPress={handleSave}>
            <Text style={styles.saveButtonText}>Save Product</Text>
          </TouchableOpacity>
        </View>
      </View>
    </View>
  );
}

// ============= PRODUCTS TAB =============
function ProductsTab() {
  const [products, setProducts] = useState([]);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');

  useEffect(() => {
    loadProducts();
  }, []);

  const loadProducts = async () => {
    try {
      const response = await axios.get(`${API_URL}/products`);
      setProducts(response.data.products);
    } catch (error) {
      Alert.alert('Error', 'Failed to load products');
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  const renderProduct = ({ item }) => (
    <View style={styles.productCard}>
      <View style={styles.productCardHeader}>
        <Text style={styles.productName}>{item.name}</Text>
        <Text style={styles.productPrice}>${item.price || '0.00'}</Text>
      </View>
      <Text style={styles.productBarcode}>{item.barcode}</Text>
      <View style={styles.productMeta}>
        <Text style={styles.productMetaText}>Brand: {item.brand || 'N/A'}</Text>
        <Text style={styles.productMetaText}>Qty: {item.quantity || 1}</Text>
        <Text style={[styles.productStatus, styles[`status${item.status}`]]}>
          {item.status || 'pending'}
        </Text>
      </View>
    </View>
  );

  return (
    <View style={styles.tabContent}>
      <View style={styles.searchBar}>
        <Ionicons name="search" size={20} color="#999" />
        <TextInput
          style={styles.searchInput}
          placeholder="Search products..."
          value={searchQuery}
          onChangeText={setSearchQuery}
        />
      </View>

      {loading ? (
        <ActivityIndicator size="large" color="#667eea" style={styles.loader} />
      ) : (
        <FlatList
          data={products.filter(p => 
            p.name?.toLowerCase().includes(searchQuery.toLowerCase()) ||
            p.barcode?.includes(searchQuery)
          )}
          renderItem={renderProduct}
          keyExtractor={(item) => item._id}
          refreshControl={
            <RefreshControl refreshing={refreshing} onRefresh={() => {
              setRefreshing(true);
              loadProducts();
            }} />
          }
          ListEmptyComponent={
            <Text style={styles.emptyText}>No products found</Text>
          }
        />
      )}
    </View>
  );
}

// ============= HISTORY TAB =============
function HistoryTab() {
  const [history, setHistory] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    loadHistory();
  }, []);

  const loadHistory = async () => {
    try {
      const response = await axios.get(`${API_URL}/history`);
      setHistory(response.data.history);
    } catch (error) {
      console.error('History error:', error);
    } finally {
      setLoading(false);
    }
  };

  const renderHistoryItem = ({ item }) => (
    <View style={styles.historyItem}>
      <View style={styles.historyIcon}>
        <Ionicons 
          name={item.action === 'scan' ? 'barcode' : 'create'} 
          size={24} 
          color="#667eea" 
        />
      </View>
      <View style={styles.historyContent}>
        <Text style={styles.historyTitle}>
          {item.productId?.name || item.barcode}
        </Text>
        <Text style={styles.historyAction}>{item.action}</Text>
        <Text style={styles.historyTime}>
          {new Date(item.timestamp).toLocaleString()}
        </Text>
      </View>
    </View>
  );

  return (
    <View style={styles.tabContent}>
      {loading ? (
        <ActivityIndicator size="large" color="#667eea" style={styles.loader} />
      ) : (
        <FlatList
          data={history}
          renderItem={renderHistoryItem}
          keyExtractor={(item) => item._id}
          ListEmptyComponent={
            <Text style={styles.emptyText}>No scan history</Text>
          }
        />
      )}
    </View>
  );
}

// ============= SETTINGS TAB =============
function SettingsTab() {
  const [apiProvider, setApiProvider] = useState('openFood');
  const [airtableConfig, setAirtableConfig] = useState({
    baseId: '',
    apiKey: ''
  });

  const providers = [
    { label: 'Open Food Facts (Free)', value: 'openFood' },
    { label: 'UPC ItemDB', value: 'upcItemDB' },
    { label: 'Barcode Lookup (Premium)', value: 'barcodeLookup' }
  ];

  return (
    <ScrollView style={styles.tabContent}>
      <View style={styles.settingsSection}>
        <Text style={styles.settingsTitle}>API Configuration</Text>
        
        <Text style={styles.settingsLabel}>Product Lookup Provider</Text>
        {providers.map((provider) => (
          <TouchableOpacity
            key={provider.value}
            style={styles.radioOption}
            onPress={() => setApiProvider(provider.value)}
          >
            <View style={styles.radioCircle}>
              {apiProvider === provider.value && (
                <View style={styles.radioSelected} />
              )}
            </View>
            <Text style={styles.radioLabel}>{provider.label}</Text>
          </TouchableOpacity>
        ))}
      </View>

      <View style={styles.settingsSection}>
        <Text style={styles.settingsTitle}>Airtable Integration</Text>
        
        <Text style={styles.settingsLabel}>Base ID</Text>
        <TextInput
          style={styles.settingsInput}
          placeholder="appXXXXXXXXXXXXXX"
          value={airtableConfig.baseId}
          onChangeText={(text) => setAirtableConfig({ ...airtableConfig, baseId: text })}
        />

        <Text style={styles.settingsLabel}>API Key</Text>
        <TextInput
          style={styles.settingsInput}
          placeholder="keyXXXXXXXXXXXXXX"
          value={airtableConfig.apiKey}
          onChangeText={(text) => setAirtableConfig({ ...airtableConfig, apiKey: text })}
          secureTextEntry
        />

        <TouchableOpacity style={styles.saveSettingsButton}>
          <Text style={styles.saveSettingsText}>Save Settings</Text>
        </TouchableOpacity>
      </View>

      <View style={styles.settingsSection}>
        <Text style={styles.settingsTitle}>Export Data</Text>
        
        <TouchableOpacity style={styles.exportButton}>
          <Ionicons name="download-outline" size={20} color="white" />
          <Text style={styles.exportButtonText}>Export to CSV</Text>
        </TouchableOpacity>
      </View>
    </ScrollView>
  );
}

// ============= STYLES =============
const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#f5f5f5',
  },
  safeArea: {
    flex: 1,
  },
  loadingContainer: {
    flex: 1,
    justifyContent: 'center',
    alignItems: 'center',
    backgroundColor: '#f5f5f5',
  },
  keyboardView: {
    flex: 1,
  },
  scrollContent: {
    flexGrow: 1,
    justifyContent: 'center',
  },

  // Login Styles
  loginContainer: {
    flex: 1,
    justifyContent: 'center',
    padding: 20,
  },
  logoContainer: {
    alignItems: 'center',
    marginBottom: 40,
  },
  appTitle: {
    fontSize: 28,
    fontWeight: 'bold',
    color: 'white',
    marginTop: 15,
  },
  appSubtitle: {
    fontSize: 14,
    color: 'rgba(255, 255, 255, 0.8)',
    marginTop: 5,
  },
  formContainer: {
    backgroundColor: 'white',
    borderRadius: 20,
    padding: 25,
    shadowColor: '#000',
    shadowOffset: { width: 0, height: 10 },
    shadowOpacity: 0.1,
    shadowRadius: 20,
    elevation: 5,
  },
  inputGroup: {
    flexDirection: 'row',
    alignItems: 'center',
    borderBottomWidth: 1,
    borderBottomColor: '#e0e0e0',
    marginBottom: 20,
  },
  inputIcon: {
    marginRight: 10,
  },
  input: {
    flex: 1,
    paddingVertical: 12,
    fontSize: 16,
  },
  authButton: {
    backgroundColor: '#667eea',
    borderRadius: 10,
    paddingVertical: 15,
    alignItems: 'center',
    marginTop: 10,
  },
  authButtonText: {
    color: 'white',
    fontSize: 16,
    fontWeight: '600',
  },
  switchText: {
    textAlign: 'center',
    marginTop: 20,
    color: '#667eea',
    fontSize: 14,
  },

  // Header Styles
  header: {
    paddingTop: 20,
    paddingHorizontal: 20,
    paddingBottom: 15,
  },
  headerContent: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: 20,
  },
  headerTitle: {
    fontSize: 24,
    fontWeight: 'bold',
    color: 'white',
  },
  statsContainer: {
    marginHorizontal: -5,
  },
  statCard: {
    backgroundColor: 'rgba(255, 255, 255, 0.2)',
    borderRadius: 15,
    padding: 15,
    marginHorizontal: 5,
    minWidth: 120,
    alignItems: 'center',
  },
  statValue: {
    fontSize: 28,
    fontWeight: 'bold',
    color: 'white',
  },
  statLabel: {
    fontSize: 12,
    color: 'rgba(255, 255, 255, 0.8)',
    marginTop: 5,
  },

  // Content
  content: {
    flex: 1,
    backgroundColor: '#f5f5f5',
  },
  tabContent: {
    flex: 1,
    padding: 20,
  },

  // Scanner Styles
  scanButton: {
    marginBottom: 20,
  },
  scanButtonGradient: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    paddingVertical: 20,
    borderRadius: 15,
  },
  scanButtonText: {
    color: 'white',
    fontSize: 18,
    fontWeight: '600',
    marginLeft: 10,
  },
  manualEntry: {
    flexDirection: 'row',
    marginBottom: 20,
  },
  manualInput: {
    flex: 1,
    backgroundColor: 'white',
    borderRadius: 10,
    paddingHorizontal: 15,
    marginRight: 10,
    fontSize: 16,
  },
  lookupButton: {
    backgroundColor: '#667eea',
    paddingHorizontal: 25,
    borderRadius: 10,
    justifyContent: 'center',
  },
  lookupButtonText: {
    color: 'white',
    fontWeight: '600',
  },
  scannerContainer: {
    flex: 1,
  },
  scannerOverlay: {
    ...StyleSheet.absoluteFillObject,
    justifyContent: 'center',
    alignItems: 'center',
  },
  scannerFrame: {
    width: 250,
    height: 250,
    borderWidth: 3,
    borderColor: '#00d2ff',
    borderRadius: 20,
  },
  cancelScanButton: {
    position: 'absolute',
    bottom: 50,
    backgroundColor: 'white',
    paddingHorizontal: 30,
    paddingVertical: 15,
    borderRadius: 25,
  },
  cancelScanText: {
    fontSize: 16,
    fontWeight: '600',
  },

  // Modal Styles
  modalContainer: {
    flex: 1,
    backgroundColor: 'rgba(0, 0, 0, 0.5)',
    justifyContent: 'flex-end',
  },
  modalContent: {
    backgroundColor: 'white',
    borderTopLeftRadius: 25,
    borderTopRightRadius: 25,
    maxHeight: height * 0.9,
  },
  modalHeader: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    padding: 20,
    borderBottomWidth: 1,
    borderBottomColor: '#e0e0e0',
  },
  modalTitle: {
    fontSize: 20,
    fontWeight: '600',
  },
  modalBody: {
    padding: 20,
    maxHeight: height * 0.6,
  },
  modalFooter: {
    flexDirection: 'row',
    padding: 20,
    borderTopWidth: 1,
    borderTopColor: '#e0e0e0',
  },
  imageScroll: {
    marginBottom: 20,
  },
  productImage: {
    width: 120,
    height: 120,
    borderRadius: 10,
    marginRight: 10,
  },
  formField: {
    marginBottom: 15,
  },
  formRow: {
    flexDirection: 'row',
    marginBottom: 15,
  },
  fieldLabel: {
    fontSize: 14,
    color: '#666',
    marginBottom: 5,
  },
  fieldInput: {
    backgroundColor: '#f5f5f5',
    borderRadius: 10,
    paddingHorizontal: 15,
    paddingVertical: 12,
    fontSize: 16,
  },
  disabledInput: {
    backgroundColor: '#e0e0e0',
  },
  textArea: {
    minHeight: 80,
    textAlignVertical: 'top',
  },
  conditionChip: {
    paddingHorizontal: 15,
    paddingVertical: 8,
    borderRadius: 20,
    backgroundColor: '#f0f0f0',
    marginRight: 10,
  },
  conditionChipActive: {
    backgroundColor: '#667eea',
  },
  conditionChipText: {
    color: '#666',
  },
  conditionChipTextActive: {
    color: 'white',
  },
  cancelButton: {
    flex: 1,
    paddingVertical: 15,
    alignItems: 'center',
    backgroundColor: '#f0f0f0',
    borderRadius: 10,
    marginRight: 10,
  },
  cancelButtonText: {
    color: '#666',
    fontWeight: '600',
  },
  saveButton: {
    flex: 2,
    paddingVertical: 15,
    alignItems: 'center',
    backgroundColor: '#00b894',
    borderRadius: 10,
  },
  saveButtonText: {
    color: 'white',
    fontWeight: '600',
    fontSize: 16,
  },

  // Products Tab Styles
  searchBar: {
    flexDirection: 'row',
    alignItems: 'center',
    backgroundColor: 'white',
    borderRadius: 10,
    paddingHorizontal: 15,
    marginBottom: 15,
  },
  searchInput: {
    flex: 1,
    paddingVertical: 12,
    marginLeft: 10,
    fontSize: 16,
  },
  productCard: {
    backgroundColor: 'white',
    borderRadius: 12,
    padding: 15,
    marginBottom: 10,
    shadowColor: '#000',
    shadowOffset: { width: 0, height: 2 },
    shadowOpacity: 0.05,
    shadowRadius: 5,
    elevation: 2,
  },
  productCardHeader: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    marginBottom: 8,
  },
  productName: {
    fontSize: 16,
    fontWeight: '600',
    color: '#333',
    flex: 1,
  },
  productPrice: {
    fontSize: 18,
    fontWeight: 'bold',
    color: '#00b894',
  },
  productBarcode: {
    fontSize: 12,
    color: '#999',
    fontFamily: Platform.OS === 'ios' ? 'Courier' : 'monospace',
    marginBottom: 8,
  },
  productMeta: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
  },
  productMetaText: {
    fontSize: 12,
    color: '#666',
  },
  productStatus: {
    paddingHorizontal: 10,
    paddingVertical: 4,
    borderRadius: 12,
    fontSize: 11,
    fontWeight: '600',
    overflow: 'hidden',
  },
  statuspending: {
    backgroundColor: '#fff3cd',
    color: '#856404',
  },
  statuslisted: {
    backgroundColor: '#d4edda',
    color: '#155724',
  },
  statussold: {
    backgroundColor: '#cce5ff',
    color: '#004085',
  },
  statusreturned: {
    backgroundColor: '#f8d7da',
    color: '#721c24',
  },

  // History Tab Styles
  historyItem: {
    flexDirection: 'row',
    backgroundColor: 'white',
    borderRadius: 12,
    padding: 15,
    marginBottom: 10,
    shadowColor: '#000',
    shadowOffset: { width: 0, height: 2 },
    shadowOpacity: 0.05,
    shadowRadius: 5,
    elevation: 2,
  },
  historyIcon: {
    width: 50,
    height: 50,
    borderRadius: 25,
    backgroundColor: '#f0f3ff',
    justifyContent: 'center',
    alignItems: 'center',
    marginRight: 15,
  },
  historyContent: {
    flex: 1,
  },
  historyTitle: {
    fontSize: 16,
    fontWeight: '600',
    color: '#333',
    marginBottom: 3,
  },
  historyAction: {
    fontSize: 14,
    color: '#667eea',
    marginBottom: 3,
  },
  historyTime: {
    fontSize: 12,
    color: '#999',
  },

  // Settings Tab Styles
  settingsSection: {
    backgroundColor: 'white',
    borderRadius: 12,
    padding: 20,
    marginBottom: 20,
  },
  settingsTitle: {
    fontSize: 18,
    fontWeight: '600',
    color: '#333',
    marginBottom: 15,
  },
  settingsLabel: {
    fontSize: 14,
    color: '#666',
    marginTop: 15,
    marginBottom: 8,
  },
  settingsInput: {
    backgroundColor: '#f5f5f5',
    borderRadius: 10,
    paddingHorizontal: 15,
    paddingVertical: 12,
    fontSize: 16,
  },
  radioOption: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingVertical: 10,
  },
  radioCircle: {
    width: 20,
    height: 20,
    borderRadius: 10,
    borderWidth: 2,
    borderColor: '#667eea',
    marginRight: 10,
    justifyContent: 'center',
    alignItems: 'center',
  },
  radioSelected: {
    width: 10,
    height: 10,
    borderRadius: 5,
    backgroundColor: '#667eea',
  },
  radioLabel: {
    fontSize: 16,
    color: '#333',
  },
  saveSettingsButton: {
    backgroundColor: '#667eea',
    borderRadius: 10,
    paddingVertical: 15,
    alignItems: 'center',
    marginTop: 20,
  },
  saveSettingsText: {
    color: 'white',
    fontSize: 16,
    fontWeight: '600',
  },
  exportButton: {
    flexDirection: 'row',
    backgroundColor: '#00b894',
    borderRadius: 10,
    paddingVertical: 15,
    alignItems: 'center',
    justifyContent: 'center',
  },
  exportButtonText: {
    color: 'white',
    fontSize: 16,
    fontWeight: '600',
    marginLeft: 8,
  },

  // Bottom Navigation
  bottomNav: {
    flexDirection: 'row',
    backgroundColor: 'white',
    borderTopWidth: 1,
    borderTopColor: '#e0e0e0',
    paddingBottom: Platform.OS === 'ios' ? 20 : 10,
    paddingTop: 10,
  },
  navItem: {
    flex: 1,
    alignItems: 'center',
  },
  navText: {
    fontSize: 11,
    color: '#999',
    marginTop: 4,
  },
  navTextActive: {
    color: '#667eea',
  },

  // Common
  loader: {
    marginTop: 50,
  },
  emptyText: {
    textAlign: 'center',
    color: '#999',
    fontSize: 16,
    marginTop: 50,
  },
});

// ============= PACKAGE.JSON =============
/*
{
  "name": "inventory-scanner-app",
  "version": "1.0.0",
  "main": "node_modules/expo/AppEntry.js",
  "scripts": {
    "start": "expo start",
    "android": "expo start --android",
    "ios": "expo start --ios",
    "web": "expo start --web"
  },
  "dependencies": {
    "expo": "~49.0.0",
    "expo-status-bar": "~1.6.0",
    "expo-camera": "~13.4.4",
    "expo-barcode-scanner": "~12.5.3",
    "expo-image-picker": "~14.3.2",
    "expo-linear-gradient": "~12.3.0",
    "react": "18.2.0",
    "react-native": "0.72.5",
    "@react-native-async-storage/async-storage": "1.18.2",
    "axios": "^1.4.0",
    "@expo/vector-icons": "^13.0.0"
  },
  "devDependencies": {
    "@babel/core": "^7.20.0"
  },
  "private": true
}
*/

// ============= APP.JSON =============
/*
{
  "expo": {
    "name": "Inventory Scanner Pro",
    "slug": "inventory-scanner",
    "version": "1.0.0",
    "orientation": "portrait",
    "icon": "./assets/icon.png",
    "userInterfaceStyle": "light",
    "splash": {
      "image": "./assets/splash.png",
      "resizeMode": "contain",
      "backgroundColor": "#667eea"
    },
    "assetBundlePatterns": [
      "**/*"
    ],
    "ios": {
      "supportsTablet": true,
      "bundleIdentifier": "com.yourcompany.inventoryscanner",
      "infoPlist": {
        "NSCameraUsageDescription": "This app needs camera access to scan barcodes",
        "NSPhotoLibraryUsageDescription": "This app needs photo library access to save product images"
      }
    },
    "android": {
      "adaptiveIcon": {
        "foregroundImage": "./assets/adaptive-icon.png",
        "backgroundColor": "#667eea"
      },
      "package": "com.yourcompany.inventoryscanner",
      "permissions": [
        "CAMERA",
        "READ_EXTERNAL_STORAGE",
        "WRITE_EXTERNAL_STORAGE"
      ]
    },
    "web": {
      "favicon": "./assets/favicon.png"
    }
  }
}
*/