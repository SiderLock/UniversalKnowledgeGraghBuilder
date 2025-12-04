"""
Universal Knowledge Graph Builder - GUI Application
Main entry point for the visual knowledge graph construction tool.
"""

import tkinter as tk
from tkinter import ttk, scrolledtext, filedialog, messagebox
import os
import sys
import webbrowser
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from kg_builder import KnowledgeGraph, KnowledgeGraphExtractor, KnowledgeGraphVisualizer, Config


class KnowledgeGraphBuilderGUI:
    """Main GUI application for building knowledge graphs."""

    def __init__(self, root):
        """Initialize the GUI application.
        
        Args:
            root: Tkinter root window
        """
        self.root = root
        self.root.title("Universal Knowledge Graph Builder")
        self.root.geometry("1000x700")
        
        # Initialize components
        self.config = Config()
        self.kg = KnowledgeGraph()
        self.extractor = KnowledgeGraphExtractor(
            llm_client=self.config.get_llm_client(),
            model=self.config.default_model
        )
        self.visualizer = KnowledgeGraphVisualizer(self.kg)
        
        # Setup UI
        self.setup_ui()
        
        # Status
        self.update_status("Ready")

    def setup_ui(self):
        """Setup the user interface."""
        # Create main container
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(2, weight=1)
        
        # Title
        title_label = ttk.Label(main_frame, text="Universal Knowledge Graph Builder", 
                               font=('Helvetica', 16, 'bold'))
        title_label.grid(row=0, column=0, pady=10)
        
        # Control panel
        self.setup_control_panel(main_frame)
        
        # Input/Output area
        self.setup_io_area(main_frame)
        
        # Status bar
        self.setup_status_bar(main_frame)

    def setup_control_panel(self, parent):
        """Setup control panel with buttons and settings."""
        control_frame = ttk.LabelFrame(parent, text="Controls", padding="10")
        control_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=5)
        
        # Domain selection
        ttk.Label(control_frame, text="Domain:").grid(row=0, column=0, sticky=tk.W, padx=5)
        self.domain_var = tk.StringVar(value=self.config.default_domain)
        domain_combo = ttk.Combobox(control_frame, textvariable=self.domain_var, 
                                    values=['general', 'medical', 'finance', 'technology', 
                                           'science', 'legal', 'education'], width=15)
        domain_combo.grid(row=0, column=1, sticky=tk.W, padx=5)
        
        # LLM status indicator
        llm_status = "✓ LLM Enabled" if self.config.is_llm_configured() else "✗ LLM Disabled (Fallback mode)"
        self.llm_label = ttk.Label(control_frame, text=llm_status, 
                                   foreground='green' if self.config.is_llm_configured() else 'orange')
        self.llm_label.grid(row=0, column=2, sticky=tk.W, padx=20)
        
        # Buttons
        button_frame = ttk.Frame(control_frame)
        button_frame.grid(row=1, column=0, columnspan=4, pady=10)
        
        ttk.Button(button_frame, text="Extract from Text", 
                  command=self.extract_from_text).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Load from File", 
                  command=self.load_from_file).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Visualize", 
                  command=self.visualize_graph).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Save Graph", 
                  command=self.save_graph).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Load Graph", 
                  command=self.load_graph).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Clear", 
                  command=self.clear_graph).pack(side=tk.LEFT, padx=5)

    def setup_io_area(self, parent):
        """Setup input/output text areas."""
        io_frame = ttk.Frame(parent)
        io_frame.grid(row=2, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=5)
        io_frame.columnconfigure(0, weight=1)
        io_frame.columnconfigure(1, weight=1)
        io_frame.rowconfigure(0, weight=1)
        
        # Input text area
        input_frame = ttk.LabelFrame(io_frame, text="Input Text", padding="5")
        input_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(0, 5))
        input_frame.columnconfigure(0, weight=1)
        input_frame.rowconfigure(0, weight=1)
        
        self.input_text = scrolledtext.ScrolledText(input_frame, wrap=tk.WORD, 
                                                    height=20, font=('Courier', 10))
        self.input_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.input_text.insert('1.0', 
            "Enter text here to extract knowledge graph...\n\n"
            "Example:\n"
            "Python is a programming language. It was created by Guido van Rossum. "
            "Python is used for web development, data science, and automation.")
        
        # Output/Graph info area
        output_frame = ttk.LabelFrame(io_frame, text="Knowledge Graph Info", padding="5")
        output_frame.grid(row=0, column=1, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(5, 0))
        output_frame.columnconfigure(0, weight=1)
        output_frame.rowconfigure(0, weight=1)
        
        self.output_text = scrolledtext.ScrolledText(output_frame, wrap=tk.WORD, 
                                                     height=20, font=('Courier', 10))
        self.output_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

    def setup_status_bar(self, parent):
        """Setup status bar at the bottom."""
        status_frame = ttk.Frame(parent)
        status_frame.grid(row=3, column=0, sticky=(tk.W, tk.E), pady=(5, 0))
        
        self.status_label = ttk.Label(status_frame, text="Ready", relief=tk.SUNKEN)
        self.status_label.pack(side=tk.LEFT, fill=tk.X, expand=True)

    def update_status(self, message):
        """Update status bar message."""
        self.status_label.config(text=message)
        self.root.update_idletasks()

    def extract_from_text(self):
        """Extract knowledge graph from input text."""
        try:
            self.update_status("Extracting knowledge graph...")
            
            # Get input text
            text = self.input_text.get('1.0', tk.END).strip()
            if not text or text.startswith("Enter text here"):
                messagebox.showwarning("No Input", "Please enter some text to extract.")
                self.update_status("Ready")
                return
            
            # Extract
            domain = self.domain_var.get()
            extraction = self.extractor.extract_from_text(text, domain)
            
            # Merge into graph
            self.kg.merge_from_extraction(extraction)
            
            # Update output
            self.update_graph_info()
            
            self.update_status(f"Extracted {len(extraction.get('entities', []))} entities "
                             f"and {len(extraction.get('relationships', []))} relationships")
            messagebox.showinfo("Success", "Knowledge graph extracted successfully!")
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to extract: {str(e)}")
            self.update_status("Error occurred")

    def load_from_file(self):
        """Load text from a file and extract knowledge graph."""
        try:
            filename = filedialog.askopenfilename(
                title="Select text file",
                filetypes=[("Text files", "*.txt"), ("All files", "*.*")]
            )
            
            if not filename:
                return
            
            with open(filename, 'r', encoding='utf-8') as f:
                text = f.read()
            
            self.input_text.delete('1.0', tk.END)
            self.input_text.insert('1.0', text)
            
            self.extract_from_text()
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load file: {str(e)}")

    def visualize_graph(self):
        """Visualize the knowledge graph."""
        try:
            if self.kg.graph.number_of_nodes() == 0:
                messagebox.showwarning("Empty Graph", "No entities in the knowledge graph yet.")
                return
            
            self.update_status("Generating visualization...")
            
            # Create output directory
            output_dir = Path(self.config.output_dir)
            output_dir.mkdir(exist_ok=True)
            
            # Generate interactive visualization
            output_path = output_dir / "knowledge_graph.html"
            self.visualizer.visualize_interactive(str(output_path))
            
            # Open in browser
            webbrowser.open('file://' + str(output_path.absolute()))
            
            self.update_status("Visualization opened in browser")
            messagebox.showinfo("Success", f"Visualization saved to: {output_path}")
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to visualize: {str(e)}")
            self.update_status("Error occurred")

    def save_graph(self):
        """Save knowledge graph to JSON file."""
        try:
            if self.kg.graph.number_of_nodes() == 0:
                messagebox.showwarning("Empty Graph", "No entities in the knowledge graph yet.")
                return
            
            filename = filedialog.asksaveasfilename(
                title="Save knowledge graph",
                defaultextension=".json",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
            )
            
            if not filename:
                return
            
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(self.kg.to_json())
            
            self.update_status(f"Graph saved to {filename}")
            messagebox.showinfo("Success", f"Knowledge graph saved to: {filename}")
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save: {str(e)}")

    def load_graph(self):
        """Load knowledge graph from JSON file."""
        try:
            filename = filedialog.askopenfilename(
                title="Load knowledge graph",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
            )
            
            if not filename:
                return
            
            with open(filename, 'r', encoding='utf-8') as f:
                self.kg.from_json(f.read())
            
            self.update_graph_info()
            self.update_status(f"Graph loaded from {filename}")
            messagebox.showinfo("Success", "Knowledge graph loaded successfully!")
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load: {str(e)}")

    def clear_graph(self):
        """Clear the knowledge graph."""
        if self.kg.graph.number_of_nodes() == 0:
            return
        
        if messagebox.askyesno("Confirm", "Are you sure you want to clear the knowledge graph?"):
            self.kg.clear()
            self.update_graph_info()
            self.update_status("Graph cleared")

    def update_graph_info(self):
        """Update the output text with graph information."""
        self.output_text.delete('1.0', tk.END)
        
        # Get summary
        summary = self.visualizer.get_graph_summary()
        self.output_text.insert('1.0', summary + "\n\n")
        
        # Add entities
        self.output_text.insert(tk.END, "Entities:\n")
        self.output_text.insert(tk.END, "-" * 40 + "\n")
        for entity in self.kg.get_entities():
            self.output_text.insert(tk.END, 
                f"• {entity.get('label', entity['id'])} ({entity.get('type', 'Entity')})\n")
        
        # Add relationships
        self.output_text.insert(tk.END, "\n\nRelationships:\n")
        self.output_text.insert(tk.END, "-" * 40 + "\n")
        for rel in self.kg.get_relationships():
            self.output_text.insert(tk.END, 
                f"• {rel['source']} --[{rel.get('relation', 'related_to')}]--> {rel['target']}\n")


def main():
    """Main entry point for the application."""
    root = tk.Tk()
    app = KnowledgeGraphBuilderGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
