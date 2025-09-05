// Airtable Automation Script for Two-Way Sync
// Prevents sync loops by checking last sync timestamps

// Configuration
const WEBHOOK_URL = 'https://webhook.4more.ca/webhook/airtable';
const WEBHOOK_SECRET = 'your_webhook_secret'; // Match your .env file
const SYNC_THRESHOLD_MINUTES = 1; // Skip if synced within last minute

// Get the record that triggered the automation
let inputConfig = input.config();
let recordId = inputConfig.recordId;
let triggerType = inputConfig.triggerType; // 'create', 'update', or 'delete'

// Get the table
let table = base.getTable("Products"); // Replace with your table name

// Function to check if we should sync
async function shouldSync(record) {
    // For new records, always sync
    if (triggerType === 'create') {
        return true;
    }
    
    // Check last WooCommerce sync timestamp
    let lastWooSync = record.getCellValue("Last WooCommerce Sync");
    
    if (lastWooSync) {
        let lastSyncTime = new Date(lastWooSync);
        let now = new Date();
        let minutesSinceSync = (now - lastSyncTime) / (1000 * 60);
        
        // Skip if synced from WooCommerce recently
        if (minutesSinceSync < SYNC_THRESHOLD_MINUTES) {
            console.log(`Skipping - synced from WooCommerce ${minutesSinceSync.toFixed(2)} minutes ago`);
            return false;
        }
    }
    
    return true;
}

// Main execution
if (recordId) {
    // Fetch the record
    let record = await table.selectRecordAsync(recordId);
    
    if (record) {
        // Check if we should sync
        let shouldSyncRecord = await shouldSync(record);
        
        if (!shouldSyncRecord) {
            console.log('Skipping sync to prevent loop');
            output.set('status', 'skipped');
        } else {
            // Prepare webhook payload
            let webhookData = {
                type: `record.${triggerType}d`, // 'record.created', 'record.updated', etc.
                base: {
                    id: base.id
                },
                record: {
                    id: record.id
                }
            };
            
            // Add specific fields for logging
            if (triggerType !== 'delete') {
                webhookData.record.fields = {
                    'Product Name': record.getCellValueAsString("Product Name"),
                    'SKU': record.getCellValueAsString("SKU"),
                    'Stock Quantity': record.getCellValue("Stock Quantity")
                };
            }
            
            // Send webhook
            try {
                let response = await fetch(WEBHOOK_URL, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'X-Airtable-Signature': WEBHOOK_SECRET
                    },
                    body: JSON.stringify(webhookData)
                });
                
                if (response.ok) {
                    let result = await response.json();
                    console.log('Webhook sent successfully:', result);
                    output.set('status', 'success');
                    output.set('response', JSON.stringify(result));
                } else {
                    let error = await response.text();
                    console.error('Webhook failed:', error);
                    output.set('status', 'error');
                    output.set('error', error);
                }
            } catch (error) {
                console.error('Failed to send webhook:', error.toString());
                output.set('status', 'error');
                output.set('error', error.toString());
            }
        }
    } else {
        console.log('Record not found');
        output.set('status', 'record_not_found');
    }
} else {
    console.log('No record ID provided');
    output.set('status', 'no_record_id');
}

// ===== SETUP INSTRUCTIONS =====
// 
// 1. In Airtable, go to Automations
// 2. Create THREE automations:
//
// AUTOMATION 1: When Record Created
// - Trigger: When record is created in Products table
// - Action: Run this script
// - Input variables:
//   - recordId: Record ID from trigger
//   - triggerType: 'create' (static text)
//
// AUTOMATION 2: When Record Updated
// - Trigger: When record is updated in Products table
// - Watched fields: All fields EXCEPT "Last WooCommerce Sync"
// - Action: Run this script
// - Input variables:
//   - recordId: Record ID from trigger
//   - triggerType: 'update' (static text)
//
// AUTOMATION 3: When Record Deleted (optional)
// - Trigger: When record enters a view (create a "Deleted" view with a checkbox field)
// - Action: Run this script
// - Input variables:
//   - recordId: Record ID from trigger
//   - triggerType: 'delete' (static text)
//
// 3. Make sure your Products table has these fields:
//    - Last WooCommerce Sync (Date time field)
//    - WooCommerce ID (Number or text field)
//    - Created From WooCommerce (Date time field, optional)