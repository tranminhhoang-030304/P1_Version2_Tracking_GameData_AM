"use client"

import { useState, useEffect } from "react"
import { Sidebar } from "@/components/sidebar"
import { Save, Plus, Trash2, Package, Server, Settings } from "lucide-react"

// Dữ liệu mặc định
const defaultItems = [
  { id: 1, key: "XP_BOOST_2X", name: "Double XP (1h)", price: 50 },
  { id: 2, key: "LIFE_POTION", name: "Extra Life Potion", price: 120 },
  { id: 3, key: "SHIELD_MAX", name: "Iron Shield", price: 300 },
]

export default function SettingsPage() {
  const [items, setItems] = useState<any[]>([])
  const [isLoaded, setIsLoaded] = useState(false)

  // Load dữ liệu từ LocalStorage
  useEffect(() => {
    const savedItems = localStorage.getItem("gameItems")
    if (savedItems) setItems(JSON.parse(savedItems))
    else setItems(defaultItems)
    setIsLoaded(true)
  }, [])

  // Lưu dữ liệu khi thay đổi
  useEffect(() => {
    if (isLoaded) localStorage.setItem("gameItems", JSON.stringify(items))
  }, [items, isLoaded])
  
  // State form nhập liệu
  const [newItemId, setNewItemId] = useState("")
  const [newItemKey, setNewItemKey] = useState("")
  const [newItemName, setNewItemName] = useState("")
  const [newItemPrice, setNewItemPrice] = useState("")
  const [isAdding, setIsAdding] = useState(false)

  const handleAddItem = () => {
    if (!newItemId || !newItemKey || !newItemName || !newItemPrice) return alert("Vui lòng nhập đủ thông tin!")
    if (items.some((item: any) => item.id === Number(newItemId))) return alert("ID này đã tồn tại!")
    
    setItems([...items, { id: Number(newItemId), key: newItemKey, name: newItemName, price: Number(newItemPrice) }])
    
    setNewItemId(""); setNewItemKey(""); setNewItemName(""); setNewItemPrice(""); setIsAdding(false) 
  }

  const handleDelete = (id: number) => {
    if (confirm("Bạn có chắc muốn xóa vật phẩm này?")) {
      setItems(items.filter((item: any) => item.id !== id))
    }
  }

  if (!isLoaded) return null

  return (
    <div className="min-h-screen bg-background text-foreground flex">
      <Sidebar />
      <main className="flex-1 pl-64 transition-all duration-300">
        <div className="p-8 space-y-8">
          
          {/* Header */}
          <div>
            <h1 className="text-3xl font-bold tracking-tight text-primary neon-text-cyan">
              Settings
            </h1>
            <p className="text-muted-foreground mt-1">
              Cấu hình hệ thống & Quản lý vật phẩm Game
            </p>
          </div>

          {/* --- PHẦN 1: SYSTEM CONFIGURATION (Đã thêm lại) --- */}
          <div className="rounded-xl border bg-card text-card-foreground shadow p-6 space-y-6">
            <h2 className="text-lg font-semibold flex items-center">
               <Server className="mr-2 h-5 w-5 text-blue-500" />
               System Configuration
            </h2>
            
            <div className="grid gap-6 md:grid-cols-2">
              <div className="space-y-2">
                <label className="text-sm font-medium">Oracle IP Address</label>
                <input 
                  type="text" 
                  defaultValue="192.168.1.100" 
                  className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Oracle Port</label>
                <input 
                  type="text" 
                  defaultValue="1521" 
                  className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                />
              </div>
              <div className="space-y-2 md:col-span-2">
                <label className="text-sm font-medium">ETL Schedule (Cron)</label>
                <input 
                  type="text" 
                  defaultValue="0 * * * *" 
                  className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                />
                <p className="text-xs text-muted-foreground">Current: Runs every hour at minute 0</p>
              </div>
            </div>

            <button className="inline-flex items-center justify-center rounded-md text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:opacity-50 disabled:pointer-events-none ring-offset-background bg-primary text-primary-foreground hover:bg-primary/90 h-10 px-4 py-2">
              <Save className="mr-2 h-4 w-4" /> Save Configuration
            </button>
          </div>

          {/* --- PHẦN 2: GAME ITEMS MANAGER --- */}
          <div className="rounded-xl border bg-card text-card-foreground shadow p-6">
            <div className="flex items-center justify-between mb-6">
              <div>
                <h2 className="text-lg font-semibold flex items-center">
                  <Package className="mr-2 h-5 w-5 text-purple-500" />
                  Game Items Manager
                </h2>
                <p className="text-sm text-muted-foreground">Danh sách vật phẩm đang bán trong Shop</p>
              </div>
              <button 
                onClick={() => setIsAdding(!isAdding)}
                className="inline-flex items-center justify-center rounded-md text-sm font-medium bg-green-600 text-white hover:bg-green-700 h-9 px-4 py-2"
              >
                <Plus className="mr-2 h-4 w-4" /> Add Item
              </button>
            </div>

            {/* Form thêm mới */}
            {isAdding && (
              <div className="mb-6 p-4 border rounded-lg bg-muted/30 animate-in fade-in slide-in-from-top-2">
                <div className="grid gap-4 md:grid-cols-5 items-end">
                  <div className="space-y-2">
                    <label className="text-sm font-medium">ID (Số)</label>
                    <input type="number" value={newItemId} onChange={(e) => setNewItemId(e.target.value)} placeholder="VD: 101" className="flex h-9 w-full rounded-md border border-input bg-background px-3 py-1 text-sm shadow-sm" />
                  </div>
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Key (Mã)</label>
                    <input value={newItemKey} onChange={(e) => setNewItemKey(e.target.value)} placeholder="VD: MAG_SWORD" className="flex h-9 w-full rounded-md border border-input bg-background px-3 py-1 text-sm shadow-sm" />
                  </div>
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Tên hiển thị</label>
                    <input value={newItemName} onChange={(e) => setNewItemName(e.target.value)} placeholder="VD: Magic Sword" className="flex h-9 w-full rounded-md border border-input bg-background px-3 py-1 text-sm shadow-sm" />
                  </div>
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Giá (Coin)</label>
                    <input type="number" value={newItemPrice} onChange={(e) => setNewItemPrice(e.target.value)} placeholder="VD: 500" className="flex h-9 w-full rounded-md border border-input bg-background px-3 py-1 text-sm shadow-sm" />
                  </div>
                  <button onClick={handleAddItem} className="inline-flex items-center justify-center rounded-md text-sm font-medium bg-primary text-primary-foreground hover:bg-primary/90 h-9 px-4">Lưu lại</button>
                </div>
              </div>
            )}

            {/* Bảng dữ liệu */}
            <div className="relative w-full overflow-auto border rounded-lg">
              <table className="w-full caption-bottom text-sm text-left">
                <thead className="bg-muted/50 [&_tr]:border-b">
                  <tr className="border-b transition-colors">
                    <th className="h-10 px-4 align-middle font-medium text-muted-foreground">ID</th>
                    <th className="h-10 px-4 align-middle font-medium text-muted-foreground">Key</th>
                    <th className="h-10 px-4 align-middle font-medium text-muted-foreground">Tên hiển thị</th>
                    <th className="h-10 px-4 align-middle font-medium text-muted-foreground">Giá (Coin)</th>
                    <th className="h-10 px-4 align-middle font-medium text-muted-foreground text-right">Thao tác</th>
                  </tr>
                </thead>
                <tbody className="[&_tr:last-child]:border-0">
                  {items.map((item: any) => (
                    <tr key={item.id} className="border-b transition-colors hover:bg-muted/50">
                      <td className="p-4 align-middle font-mono text-xs font-bold">{item.id}</td>
                      <td className="p-4 align-middle font-mono text-xs text-blue-400">{item.key}</td>
                      <td className="p-4 align-middle font-medium text-primary">{item.name}</td>
                      <td className="p-4 align-middle font-bold text-yellow-500">{item.price} 🪙</td>
                      <td className="p-4 align-middle text-right">
                        <button onClick={() => handleDelete(item.id)} className="text-red-500 hover:text-red-700 hover:bg-red-100/10 p-2 rounded-full transition-colors"><Trash2 className="h-4 w-4" /></button>
                      </td>
                    </tr>
                  ))}
                  {items.length === 0 && (
                    <tr><td colSpan={5} className="p-8 text-center text-muted-foreground">Chưa có dữ liệu.</td></tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </main>
    </div>
  )
}