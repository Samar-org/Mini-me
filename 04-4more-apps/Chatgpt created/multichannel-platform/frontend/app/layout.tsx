import "../styles/globals.css";
export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="min-h-screen">
        <nav className="sticky top-0 bg-white border-b px-4 py-3 flex items-center justify-between">
          <div className="font-bold">Multichannel Platform</div>
          <div className="text-sm text-gray-500">Demo</div>
        </nav>
        <main className="p-4 max-w-6xl mx-auto">{children}</main>
      </body>
    </html>
  );
}
