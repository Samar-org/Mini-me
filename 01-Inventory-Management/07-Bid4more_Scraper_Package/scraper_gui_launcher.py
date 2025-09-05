import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog
import threading
import subprocess
import sys
import os
import json
from datetime import datetime
import queue
import logging
from pathlib import Path

class ScraperGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Bid4more Product Scraper - v1.0")
        self.root.geometry("900x700")
        self.root.resizable(True, True)
        
        # Configure style
        style = ttk.Style()
        style.theme_use('clam')
        
        # Queue for thread communication
        self.message_queue = queue.Queue()
        
        # Track running processes
        self.running_processes = {}
        
        # Setup logging
        self.setup_logging()
        
        # Create GUI
        self.create_widgets()
        
        # Start checking for messages
        self.check_queue()
        
        # Load configuration
        self.load_config()
    
    def setup_logging(self):
        """Setup logging for the GUI."""
        # Create logs directory if it doesn't exist
        logs_dir = Path("logs")
        logs_dir.mkdir(exist_ok=True)
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(logs_dir / "gui_scraper.log"),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def create_widgets(self):
        """Create all GUI widgets."""
        # Main frame
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(4, weight=1)
        
        # Title
        title_label = ttk.Label(main_frame, text="Bid4more Product Scraper", 
                               font=('Arial', 16, 'bold'))
        title_label.grid(row=0, column=0, columnspan=3, pady=(0, 20))
        
        # Configuration section
        config_frame = ttk.LabelFrame(main_frame, text="Configuration", padding="10")
        config_frame.grid(row=1, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 10))
        config_frame.columnconfigure(1, weight=1)
        
        # Config file path
        ttk.Label(config_frame, text="Config File:").grid(row=0, column=0, sticky=tk.W, padx=(0, 5))
        self.config_path = tk.StringVar(value=".env")
        ttk.Entry(config_frame, textvariable=self.config_path, width=50).grid(row=0, column=1, sticky=(tk.W, tk.E), padx=(0, 5))
        ttk.Button(config_frame, text="Browse", command=self.browse_config).grid(row=0, column=2)
        
        # Status indicator
        self.status_label = ttk.Label(config_frame, text="Status: Ready", foreground="green")
        self.status_label.grid(row=1, column=0, columnspan=3, sticky=tk.W, pady=(5, 0))
        
        # Scraper buttons section
        scrapers_frame = ttk.LabelFrame(main_frame, text="Available Scrapers", padding="10")
        scrapers_frame.grid(row=2, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 10))
        scrapers_frame.columnconfigure(0, weight=1)
        scrapers_frame.columnconfigure(1, weight=1)
        scrapers_frame.columnconfigure(2, weight=1)
        
        # Scraper 1: URL Scraper
        scraper1_frame = ttk.Frame(scrapers_frame, relief='ridge', padding="10")
        scraper1_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(0, 5))
        
        ttk.Label(scraper1_frame, text="URL Scraper", font=('Arial', 12, 'bold')).pack()
        ttk.Label(scraper1_frame, text="Scrapes product info from\nAmazon & Walmart URLs", 
                 justify=tk.CENTER, wraplength=200).pack(pady=(5, 10))
        
        self.url_scraper_btn = ttk.Button(scraper1_frame, text="Run URL Scraper", 
                                         command=lambda: self.run_scraper("url_scraper"))
        self.url_scraper_btn.pack(fill=tk.X)
        
        self.url_status = ttk.Label(scraper1_frame, text="Ready", foreground="green")
        self.url_status.pack(pady=(5, 0))
        
        # Scraper 2: Image Search Scraper
        scraper2_frame = ttk.Frame(scrapers_frame, relief='ridge', padding="10")
        scraper2_frame.grid(row=0, column=1, sticky=(tk.W, tk.E, tk.N, tk.S), padx=5)
        
        ttk.Label(scraper2_frame, text="Image Search", font=('Arial', 12, 'bold')).pack()
        ttk.Label(scraper2_frame, text="Finds products using\nGoogle Lens-style search", 
                 justify=tk.CENTER, wraplength=200).pack(pady=(5, 10))
        
        self.image_scraper_btn = ttk.Button(scraper2_frame, text="Run Image Search", 
                                           command=lambda: self.run_scraper("image_scraper"))
        self.image_scraper_btn.pack(fill=tk.X)
        
        self.image_status = ttk.Label(scraper2_frame, text="Ready", foreground="green")
        self.image_status.pack(pady=(5, 0))
        
        # Scraper 3: Custom Scraper (placeholder for your third scraper)
        scraper3_frame = ttk.Frame(scrapers_frame, relief='ridge', padding="10")
        scraper3_frame.grid(row=0, column=2, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(5, 0))
        
        ttk.Label(scraper3_frame, text="Custom Scraper", font=('Arial', 12, 'bold')).pack()
        ttk.Label(scraper3_frame, text="Your custom scraping\nfunctionality here", 
                 justify=tk.CENTER, wraplength=200).pack(pady=(5, 10))
        
        self.custom_scraper_btn = ttk.Button(scraper3_frame, text="Run Custom Scraper", 
                                            command=lambda: self.run_scraper("custom_scraper"))
        self.custom_scraper_btn.pack(fill=tk.X)
        
        self.custom_status = ttk.Label(scraper3_frame, text="Ready", foreground="green")
        self.custom_status.pack(pady=(5, 0))
        
        # Control buttons
        control_frame = ttk.Frame(main_frame)
        control_frame.grid(row=3, column=0, columnspan=3, pady=(0, 10))
        
        ttk.Button(control_frame, text="Stop All Scrapers", 
                  command=self.stop_all_scrapers).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(control_frame, text="View Logs", 
                  command=self.view_logs).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(control_frame, text="Clear Output", 
                  command=self.clear_output).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(control_frame, text="Settings", 
                  command=self.open_settings).pack(side=tk.LEFT)
        
        # Output section
        output_frame = ttk.LabelFrame(main_frame, text="Output Log", padding="10")
        output_frame.grid(row=4, column=0, columnspan=3, sticky=(tk.W, tk.E, tk.N, tk.S))
        output_frame.columnconfigure(0, weight=1)
        output_frame.rowconfigure(0, weight=1)
        
        self.output_text = scrolledtext.ScrolledText(output_frame, height=15, wrap=tk.WORD)
        self.output_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Progress bar
        self.progress = ttk.Progressbar(main_frame, mode='indeterminate')
        self.progress.grid(row=5, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(10, 0))
    
    def load_config(self):
        """Load configuration from .env file."""
        config_file = self.config_path.get()
        if os.path.exists(config_file):
            self.log_message(f"✅ Configuration file found: {config_file}")
            # Check for required environment variables
            self.check_environment()
        else:
            self.log_message(f"⚠️ Configuration file not found: {config_file}")
            self.log_message("Please create a .env file with your API keys.")
    
    def check_environment(self):
        """Check if required environment variables are set."""
        try:
            from dotenv import load_dotenv
            load_dotenv(self.config_path.get())
            
            required_vars = ['AIRTABLE_API_KEY', 'AIRTABLE_BASE_ID']
            missing_vars = []
            
            for var in required_vars:
                if not os.environ.get(var) or os.environ.get(var) == f"YOUR_{var}_HERE":
                    missing_vars.append(var)
            
            if missing_vars:
                self.log_message(f"⚠️ Missing required variables: {', '.join(missing_vars)}")
                self.status_label.config(text="Status: Configuration Incomplete", foreground="orange")
            else:
                self.log_message("✅ All required environment variables are set")
                self.status_label.config(text="Status: Ready", foreground="green")
                
        except ImportError:
            self.log_message("⚠️ python-dotenv not installed. Install with: pip install python-dotenv")
    
    def browse_config(self):
        """Browse for configuration file."""
        filename = filedialog.askopenfilename(
            title="Select Configuration File",
            filetypes=[("Environment files", "*.env"), ("All files", "*.*")]
        )
        if filename:
            self.config_path.set(filename)
            self.load_config()
    
    def run_scraper(self, scraper_type):
        """Run a specific scraper in a separate thread."""
        if scraper_type in self.running_processes:
            self.log_message(f"⚠️ {scraper_type} is already running!")
            return
        
        # Determine which script to run
        script_mapping = {
            "url_scraper": "enhanced_scraper.py",
            "image_scraper": "google_lens_scraper.py", 
            "custom_scraper": "custom_scraper.py"  # Replace with your third scraper
        }
        
        script_name = script_mapping.get(scraper_type)
        if not script_name or not os.path.exists(script_name):
            self.log_message(f"❌ Script file not found: {script_name}")
            messagebox.showerror("Error", f"Script file '{script_name}' not found in current directory.")
            return
        
        # Update UI
        self.update_scraper_status(scraper_type, "Running", "blue")
        self.progress.start(10)
        
        # Start scraper in separate thread
        thread = threading.Thread(target=self._run_scraper_thread, args=(scraper_type, script_name))
        thread.daemon = True
        thread.start()
        
        self.log_message(f"🚀 Started {scraper_type.replace('_', ' ').title()}")
    
    def _run_scraper_thread(self, scraper_type, script_name):
        """Run scraper in separate thread."""
        try:
            # Set environment variables from config file
            env = os.environ.copy()
            if os.path.exists(self.config_path.get()):
                from dotenv import load_dotenv
                load_dotenv(self.config_path.get())
                
                # Update environment with loaded variables
                for key, value in os.environ.items():
                    env[key] = value
            
            # Run the scraper
            process = subprocess.Popen(
                [sys.executable, script_name],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True,
                env=env
            )
            
            self.running_processes[scraper_type] = process
            
            # Read output line by line
            for line in iter(process.stdout.readline, ''):
                if line:
                    self.message_queue.put(('output', line.strip()))
            
            # Wait for process to complete
            return_code = process.wait()
            
            # Clean up
            if scraper_type in self.running_processes:
                del self.running_processes[scraper_type]
            
            # Update UI
            if return_code == 0:
                self.message_queue.put(('status', (scraper_type, 'Completed', 'green')))
                self.message_queue.put(('output', f"✅ {scraper_type.replace('_', ' ').title()} completed successfully!"))
            else:
                self.message_queue.put(('status', (scraper_type, 'Failed', 'red')))
                self.message_queue.put(('output', f"❌ {scraper_type.replace('_', ' ').title()} failed with code {return_code}"))
                
        except Exception as e:
            self.message_queue.put(('status', (scraper_type, 'Error', 'red')))
            self.message_queue.put(('output', f"❌ Error running {scraper_type}: {str(e)}"))
            if scraper_type in self.running_processes:
                del self.running_processes[scraper_type]
        
        finally:
            self.message_queue.put(('progress_stop', None))
    
    def update_scraper_status(self, scraper_type, status, color):
        """Update scraper status in UI."""
        status_labels = {
            "url_scraper": self.url_status,
            "image_scraper": self.image_status,
            "custom_scraper": self.custom_status
        }
        
        if scraper_type in status_labels:
            status_labels[scraper_type].config(text=status, foreground=color)
    
    def stop_all_scrapers(self):
        """Stop all running scrapers."""
        if not self.running_processes:
            self.log_message("ℹ️ No scrapers are currently running")
            return
        
        stopped_count = 0
        for scraper_type, process in list(self.running_processes.items()):
            try:
                process.terminate()
                stopped_count += 1
                self.update_scraper_status(scraper_type, "Stopped", "orange")
            except Exception as e:
                self.log_message(f"❌ Error stopping {scraper_type}: {e}")
        
        self.running_processes.clear()
        self.progress.stop()
        self.log_message(f"🛑 Stopped {stopped_count} scraper(s)")
    
    def view_logs(self):
        """Open logs directory."""
        logs_dir = Path("logs")
        if logs_dir.exists():
            if sys.platform == "win32":
                os.startfile(logs_dir)
            elif sys.platform == "darwin":
                subprocess.run(["open", logs_dir])
            else:
                subprocess.run(["xdg-open", logs_dir])
        else:
            messagebox.showinfo("Info", "No logs directory found.")
    
    def clear_output(self):
        """Clear the output text."""
        self.output_text.delete(1.0, tk.END)
        self.log_message("🗑️ Output cleared")
    
    def open_settings(self):
        """Open settings dialog."""
        SettingsDialog(self.root, self.config_path.get(), self.load_config)
    
    def log_message(self, message):
        """Add message to output log."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted_message = f"[{timestamp}] {message}\n"
        
        self.output_text.insert(tk.END, formatted_message)
        self.output_text.see(tk.END)
        
        # Also log to file
        self.logger.info(message)
    
    def check_queue(self):
        """Check for messages from worker threads."""
        try:
            while True:
                msg_type, data = self.message_queue.get_nowait()
                
                if msg_type == 'output':
                    self.log_message(data)
                elif msg_type == 'status':
                    scraper_type, status, color = data
                    self.update_scraper_status(scraper_type, status, color)
                elif msg_type == 'progress_stop':
                    self.progress.stop()
                
        except queue.Empty:
            pass
        
        # Schedule next check
        self.root.after(100, self.check_queue)

class SettingsDialog:
    def __init__(self, parent, config_path, callback):
        self.callback = callback
        self.config_path = config_path
        
        # Create dialog window
        self.dialog = tk.Toplevel(parent)
        self.dialog.title("Settings")
        self.dialog.geometry("500x400")
        self.dialog.resizable(False, False)
        self.dialog.transient(parent)
        self.dialog.grab_set()
        
        # Center the dialog
        self.dialog.geometry("+%d+%d" % (parent.winfo_rootx() + 50, parent.winfo_rooty() + 50))
        
        self.create_widgets()
        self.load_current_settings()
    
    def create_widgets(self):
        """Create settings dialog widgets."""
        main_frame = ttk.Frame(self.dialog, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Title
        ttk.Label(main_frame, text="Configuration Settings", 
                 font=('Arial', 14, 'bold')).pack(pady=(0, 20))
        
        # Settings frame
        settings_frame = ttk.LabelFrame(main_frame, text="API Configuration", padding="10")
        settings_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 20))
        
        # Configuration fields
        self.config_vars = {}
        config_fields = [
            ("Airtable API Key", "AIRTABLE_API_KEY"),
            ("Airtable Base ID", "AIRTABLE_BASE_ID"),
            ("Google API Key", "GOOGLE_API_KEY"),
            ("Google Custom Search ID", "GOOGLE_CX"),
            ("SerpAPI Key", "SERPAPI_KEY")
        ]
        
        for i, (label, key) in enumerate(config_fields):
            ttk.Label(settings_frame, text=f"{label}:").grid(row=i, column=0, sticky=tk.W, pady=2)
            
            var = tk.StringVar()
            entry = ttk.Entry(settings_frame, textvariable=var, width=40, show="*" if "key" in label.lower() else "")
            entry.grid(row=i, column=1, sticky=(tk.W, tk.E), padx=(10, 0), pady=2)
            
            self.config_vars[key] = var
            settings_frame.columnconfigure(1, weight=1)
        
        # Buttons
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X)
        
        ttk.Button(button_frame, text="Save", command=self.save_settings).pack(side=tk.RIGHT, padx=(10, 0))
        ttk.Button(button_frame, text="Cancel", command=self.dialog.destroy).pack(side=tk.RIGHT)
        ttk.Button(button_frame, text="Load from .env", command=self.load_from_env).pack(side=tk.LEFT)
    
    def load_current_settings(self):
        """Load current settings from environment."""
        try:
            from dotenv import load_dotenv
            load_dotenv(self.config_path)
            
            for key, var in self.config_vars.items():
                value = os.environ.get(key, "")
                if value and not value.startswith("YOUR_"):
                    var.set(value)
        except ImportError:
            pass
    
    def load_from_env(self):
        """Load settings from .env file."""
        if os.path.exists(self.config_path):
            self.load_current_settings()
            messagebox.showinfo("Success", "Settings loaded from .env file")
        else:
            messagebox.showwarning("Warning", f".env file not found at {self.config_path}")
    
    def save_settings(self):
        """Save settings to .env file."""
        try:
            # Read existing .env file
            env_content = []
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r') as f:
                    env_content = f.readlines()
            
            # Update or add new values
            updated_keys = set()
            for i, line in enumerate(env_content):
                if '=' in line and not line.strip().startswith('#'):
                    key = line.split('=')[0].strip()
                    if key in self.config_vars:
                        new_value = self.config_vars[key].get().strip()
                        if new_value:
                            env_content[i] = f"{key}={new_value}\n"
                            updated_keys.add(key)
            
            # Add new keys that weren't in the file
            for key, var in self.config_vars.items():
                if key not in updated_keys:
                    value = var.get().strip()
                    if value:
                        env_content.append(f"{key}={value}\n")
            
            # Write back to file
            with open(self.config_path, 'w') as f:
                f.writelines(env_content)
            
            messagebox.showinfo("Success", "Settings saved successfully!")
            
            # Reload configuration in main app
            if self.callback:
                self.callback()
            
            self.dialog.destroy()
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save settings: {str(e)}")

def main():
    """Main function to run the GUI."""
    # Check if required files exist
    required_files = ["enhanced_scraper.py", "google_lens_scraper.py"]
    missing_files = [f for f in required_files if not os.path.exists(f)]
    
    if missing_files:
        import tkinter as tk
        from tkinter import messagebox
        root = tk.Tk()
        root.withdraw()  # Hide the root window
        
        messagebox.showerror(
            "Missing Files", 
            f"The following scraper files are missing:\n\n" + 
            "\n".join(f"• {f}" for f in missing_files) +
            "\n\nPlease ensure all scraper files are in the same directory as this GUI."
        )
        return
    
    # Create and run the GUI
    root = tk.Tk()
    app = ScraperGUI(root)
    
    try:
        root.mainloop()
    except KeyboardInterrupt:
        app.stop_all_scrapers()
        root.quit()

if __name__ == "__main__":
    main()