"""
Interactive UI Dashboard for Kafka Order Processing System
Provides real-time visualization of producer, consumer, and system metrics
Uses Rich library for beautiful terminal UI
"""

import threading
import time
from datetime import datetime
from rich.console import Console
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.live import Live
from rich.align import Align
from rich.text import Text
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from rich import box
import sys
import os

# Add parent directory to path and change to project root
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
os.chdir(project_root)  # Change working directory to project root

from src.producer import OrderProducer
from src.consumer import OrderConsumer


class OrderDashboard:
    """Interactive dashboard for monitoring order processing"""
    
    def __init__(self):
        self.console = Console()
        self.producer = None
        self.consumer = None
        self.running = False
        self.start_time = None
        
        # Metrics
        self.producer_metrics = {
            'total_produced': 0,
            'last_order': None,
            'rate': 0.0
        }
        
        self.consumer_metrics = {
            'total_consumed': 0,
            'successful': 0,
            'retried': 0,
            'sent_to_dlq': 0,
            'running_avg': 0.0,
            'min_price': 0.0,
            'max_price': 0.0,
            'last_order': None
        }
        
        self.recent_orders = []
        self.max_recent = 10
    
    def initialize_kafka(self):
        """Initialize Kafka producer and consumer"""
        try:
            self.producer = OrderProducer()
            self.consumer = OrderConsumer()
            self.start_time = datetime.now()
            return True
        except Exception as e:
            self.console.print(f"[bold red]Error initializing Kafka: {e}[/bold red]")
            return False
    
    def producer_thread(self):
        """Producer thread - generates orders continuously"""
        while self.running:
            try:
                order = self.producer.produce_order()
                if order:
                    self.producer_metrics['total_produced'] += 1
                    self.producer_metrics['last_order'] = order
                    
                    # Add to recent orders
                    self.recent_orders.insert(0, {
                        'type': 'PRODUCED',
                        'order': order,
                        'timestamp': datetime.now()
                    })
                    if len(self.recent_orders) > self.max_recent:
                        self.recent_orders.pop()
                
                time.sleep(2)  # Produce every 2 seconds
            except Exception as e:
                self.console.log(f"Producer error: {e}")
    
    def consumer_thread(self):
        """Consumer thread - processes orders continuously"""
        while self.running:
            try:
                order = self.consumer.consume_messages(timeout=0.5)
                if order:
                    stats = self.consumer.get_stats()
                    self.consumer_metrics.update({
                        'total_consumed': stats['total_consumed'],
                        'successful': stats['successful'],
                        'retried': stats['retried'],
                        'sent_to_dlq': stats['sent_to_dlq'],
                        'running_avg': stats['running_avg'],
                        'min_price': stats.get('min_price', 0.0),
                        'max_price': stats.get('max_price', 0.0),
                        'last_order': order
                    })
                    
                    # Add to recent orders
                    self.recent_orders.insert(0, {
                        'type': 'CONSUMED',
                        'order': order,
                        'timestamp': datetime.now()
                    })
                    if len(self.recent_orders) > self.max_recent:
                        self.recent_orders.pop()
                        
            except Exception as e:
                self.console.log(f"Consumer error: {e}")
    
    def generate_header(self):
        """Generate header panel"""
        uptime = "00:00:00"
        if self.start_time:
            elapsed = datetime.now() - self.start_time
            uptime = str(elapsed).split('.')[0]
        
        header_text = Text()
        header_text.append("🚀 ", style="bold yellow")
        header_text.append("Kafka Order Processing System", style="bold cyan")
        header_text.append(" | Real-time Dashboard", style="bold white")
        header_text.append(f"\n⏱️  Uptime: {uptime}", style="dim")
        
        return Panel(
            Align.center(header_text),
            style="bold blue",
            box=box.DOUBLE
        )
    
    def generate_producer_panel(self):
        """Generate producer statistics panel"""
        table = Table(show_header=False, box=None, padding=(0, 2))
        table.add_column("Metric", style="cyan", width=20)
        table.add_column("Value", style="green")
        
        table.add_row("📤 Total Produced", f"{self.producer_metrics['total_produced']}")
        table.add_row("⚡ Production Rate", f"{self.producer_metrics['total_produced'] / max((datetime.now() - self.start_time).seconds, 1):.2f} msg/sec" if self.start_time else "N/A")
        
        if self.producer_metrics['last_order']:
            last = self.producer_metrics['last_order']
            table.add_row("🔖 Last Order ID", f"#{last['orderId']}")
            table.add_row("📦 Last Product", last['product'])
            table.add_row("💰 Last Price", f"${last['price']:.2f}")
        
        return Panel(
            table,
            title="[bold yellow]📊 Producer Metrics[/bold yellow]",
            border_style="yellow",
            box=box.ROUNDED
        )
    
    def generate_consumer_panel(self):
        """Generate consumer statistics panel"""
        table = Table(show_header=False, box=None, padding=(0, 2))
        table.add_column("Metric", style="cyan", width=20)
        table.add_column("Value", style="green")
        
        table.add_row("📥 Total Consumed", f"{self.consumer_metrics['total_consumed']}")
        table.add_row("✅ Successful", f"{self.consumer_metrics['successful']}")
        table.add_row("🔄 Retried", f"{self.consumer_metrics['retried']}", style="yellow")
        table.add_row("⚠️  Sent to DLQ", f"{self.consumer_metrics['sent_to_dlq']}", style="red")
        
        if self.consumer_metrics['last_order']:
            last = self.consumer_metrics['last_order']
            table.add_row("🔖 Last Order ID", f"#{last['orderId']}")
            table.add_row("📦 Last Product", last['product'])
            table.add_row("💰 Last Price", f"${last['price']:.2f}")
        
        return Panel(
            table,
            title="[bold green]📊 Consumer Metrics[/bold green]",
            border_style="green",
            box=box.ROUNDED
        )
    
    def generate_aggregation_panel(self):
        """Generate price aggregation panel"""
        table = Table(show_header=False, box=None, padding=(0, 2))
        table.add_column("Metric", style="cyan", width=20)
        table.add_column("Value", style="magenta bold")
        
        avg = self.consumer_metrics['running_avg']
        min_price = self.consumer_metrics['min_price']
        max_price = self.consumer_metrics['max_price']
        
        table.add_row("📊 Running Average", f"${avg:.2f}")
        table.add_row("📉 Minimum Price", f"${min_price:.2f}" if min_price < float('inf') else "$0.00")
        table.add_row("📈 Maximum Price", f"${max_price:.2f}")
        
        # Calculate price range
        if max_price > 0:
            range_val = max_price - (min_price if min_price < float('inf') else 0)
            table.add_row("📏 Price Range", f"${range_val:.2f}")
        
        return Panel(
            table,
            title="[bold magenta]💹 Price Aggregation Analytics[/bold magenta]",
            border_style="magenta",
            box=box.ROUNDED
        )
    
    def generate_recent_orders_panel(self):
        """Generate recent orders panel"""
        table = Table(box=box.SIMPLE, show_header=True, header_style="bold cyan")
        table.add_column("Type", style="dim", width=10)
        table.add_column("Order ID", style="cyan", width=10)
        table.add_column("Product", style="white", width=15)
        table.add_column("Price", style="green", width=10, justify="right")
        table.add_column("Time", style="dim", width=12)
        
        for item in self.recent_orders[:8]:
            order = item['order']
            timestamp = item['timestamp'].strftime("%H:%M:%S")
            type_icon = "📤" if item['type'] == 'PRODUCED' else "📥"
            type_style = "yellow" if item['type'] == 'PRODUCED' else "green"
            
            table.add_row(
                f"{type_icon} {item['type'][:4]}",
                f"#{order['orderId']}",
                order['product'][:15],
                f"${order['price']:.2f}",
                timestamp,
                style=type_style
            )
        
        if not self.recent_orders:
            table.add_row("—", "—", "No orders yet", "—", "—", style="dim")
        
        return Panel(
            table,
            title="[bold white]📜 Recent Order Activity[/bold white]",
            border_style="white",
            box=box.ROUNDED
        )
    
    def generate_system_info_panel(self):
        """Generate system information panel"""
        table = Table(show_header=False, box=None, padding=(0, 2))
        table.add_column("Info", style="cyan", width=20)
        table.add_column("Value", style="white")
        
        table.add_row("🔧 Kafka Broker", "localhost:9092")
        table.add_row("📋 Schema Registry", "localhost:8081")
        table.add_row("📌 Main Topic", "orders")
        table.add_row("🔄 Retry Topic", "orders-retry")
        table.add_row("⚠️  DLQ Topic", "orders-dlq")
        table.add_row("🔐 Serialization", "Avro")
        table.add_row("🔁 Max Retries", "3")
        
        return Panel(
            table,
            title="[bold blue]ℹ️  System Configuration[/bold blue]",
            border_style="blue",
            box=box.ROUNDED
        )
    
    def generate_layout(self):
        """Generate the complete dashboard layout"""
        layout = Layout()
        
        # Create layout structure
        layout.split_column(
            Layout(name="header", size=5),
            Layout(name="main", ratio=1),
            Layout(name="footer", size=3)
        )
        
        # Split main section
        layout["main"].split_row(
            Layout(name="left"),
            Layout(name="right")
        )
        
        layout["left"].split_column(
            Layout(name="producer"),
            Layout(name="consumer")
        )
        
        layout["right"].split_column(
            Layout(name="aggregation"),
            Layout(name="system")
        )
        
        # Update panels
        layout["header"].update(self.generate_header())
        layout["producer"].update(self.generate_producer_panel())
        layout["consumer"].update(self.generate_consumer_panel())
        layout["aggregation"].update(self.generate_aggregation_panel())
        layout["system"].update(self.generate_system_info_panel())
        
        # Footer with recent orders
        layout["footer"].update(self.generate_recent_orders_panel())
        
        return layout
    
    def run(self):
        """Run the dashboard"""
        if not self.initialize_kafka():
            return
        
        self.running = True
        
        # Start producer and consumer threads
        prod_thread = threading.Thread(target=self.producer_thread, daemon=True)
        cons_thread = threading.Thread(target=self.consumer_thread, daemon=True)
        
        prod_thread.start()
        cons_thread.start()
        
        # Run live dashboard
        try:
            with Live(self.generate_layout(), refresh_per_second=2, console=self.console) as live:
                while self.running:
                    live.update(self.generate_layout())
                    time.sleep(0.5)
        except KeyboardInterrupt:
            self.console.print("\n[bold yellow]Shutting down dashboard...[/bold yellow]")
        finally:
            self.running = False
            if self.producer:
                self.producer.close()
            if self.consumer:
                self.consumer.close()
            self.console.print("[bold green]Dashboard stopped successfully![/bold green]")


def main():
    """Main function"""
    console = Console()
    
    # Display welcome message
    console.print("\n" + "="*80, style="bold cyan")
    console.print(Align.center("🚀 KAFKA ORDER PROCESSING SYSTEM 🚀", style="bold yellow"))
    console.print(Align.center("Real-time Order Production, Consumption & Analytics", style="bold white"))
    console.print("="*80 + "\n", style="bold cyan")
    
    console.print("[bold green]Starting dashboard...[/bold green]\n")
    console.print("[dim]Press Ctrl+C to stop[/dim]\n")
    
    time.sleep(2)
    
    dashboard = OrderDashboard()
    dashboard.run()


if __name__ == '__main__':
    main()
