"use client"

import { useState, useEffect } from "react"
import { Plus, Pencil, Trash2, Loader2 } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import { Badge } from "@/components/ui/badge"

// Định nghĩa kiểu dữ liệu khớp với Backend
interface Booster {
  id: number
  booster_key: string
  booster_name: string
  coin_cost: number
}

export function BoostersCRUDTable() {
  const [boosters, setBoosters] = useState<Booster[]>([])
  const [loading, setLoading] = useState(true)
  const [isOpen, setIsOpen] = useState(false)
  const [isSaving, setIsSaving] = useState(false)

  // Form state
  const [newName, setNewName] = useState("")
  const [newCost, setNewCost] = useState("")

  // 1. Tải danh sách Booster khi vào trang
  const fetchBoosters = async () => {
    try {
      const res = await fetch("http://127.0.0.1:8000/api/settings/boosters")
      if (res.ok) {
        const data = await res.json()
        setBoosters(data)
      }
    } catch (error) {
      console.error("Failed to fetch boosters", error)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchBoosters()
  }, [])

  // 2. Hàm xử lý Thêm mới
  const handleCreate = async () => {
    if (!newName || !newCost) return

    setIsSaving(true)
    try {
      // Tạo key tự động từ tên (VD: "Giày Bay" -> "giay_bay")
      const generatedKey = newName.trim().toLowerCase().replace(/\s+/g, '_')

      const payload = {
        booster_key: generatedKey,
        booster_name: newName,
        coin_cost: parseInt(newCost) || 0
      }

      const res = await fetch("http://127.0.0.1:8000/api/settings/boosters", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      })

      if (!res.ok) {
        throw new Error("Failed to create")
      }

      // Thành công thì tải lại danh sách và đóng form
      await fetchBoosters()
      setIsOpen(false)
      setNewName("")
      setNewCost("")
      
    } catch (error) {
      alert("Lỗi khi thêm Booster! Có thể Key đã tồn tại.")
    } finally {
      setIsSaving(false)
    }
  }

  // 3. Hàm Xóa (Optional)
  const handleDelete = async (id: number) => {
    if(!confirm("Bạn có chắc muốn xóa không?")) return;
    
    // Lưu ý: Bạn cần API xóa ở backend nếu muốn chức năng này hoạt động thật
    // Hiện tại chỉ xóa trên giao diện tạm thời
    setBoosters(boosters.filter(b => b.id !== id))
  }

  return (
    <div className="rounded-md border bg-card p-4">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h2 className="text-lg font-semibold">Game Items</h2>
          <p className="text-sm text-muted-foreground">Quản lý các vật phẩm trong game</p>
        </div>
        
        <Dialog open={isOpen} onOpenChange={setIsOpen}>
          <DialogTrigger asChild>
            <Button>
              <Plus className="mr-2 h-4 w-4" /> Add Item
            </Button>
          </DialogTrigger>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>Thêm Vật Phẩm Mới</DialogTitle>
              <DialogDescription>
                Nhập thông tin vật phẩm để bán trong Shop.
              </DialogDescription>
            </DialogHeader>
            <div className="grid gap-4 py-4">
              <div className="grid grid-cols-4 items-center gap-4">
                <Label htmlFor="name" className="text-right">
                  Tên
                </Label>
                <Input
                  id="name"
                  placeholder="Ví dụ: Búa Thần"
                  value={newName}
                  onChange={(e) => setNewName(e.target.value)}
                  className="col-span-3"
                />
              </div>
              <div className="grid grid-cols-4 items-center gap-4">
                <Label htmlFor="cost" className="text-right">
                  Giá Coin
                </Label>
                <Input
                  id="cost"
                  type="number"
                  placeholder="Ví dụ: 100"
                  value={newCost}
                  onChange={(e) => setNewCost(e.target.value)}
                  className="col-span-3"
                />
              </div>
            </div>
            <DialogFooter>
              <Button onClick={handleCreate} disabled={isSaving}>
                {isSaving && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                Lưu lại
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </div>

      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>ID</TableHead>
            <TableHead>Key (Mã)</TableHead>
            <TableHead>Tên hiển thị</TableHead>
            <TableHead>Giá (Coin)</TableHead>
            <TableHead className="text-right">Thao tác</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {loading ? (
             <TableRow>
               <TableCell colSpan={5} className="text-center h-24">Đang tải...</TableCell>
             </TableRow>
          ) : boosters.length === 0 ? (
            <TableRow>
               <TableCell colSpan={5} className="text-center h-24 text-muted-foreground">Chưa có dữ liệu</TableCell>
             </TableRow>
          ) : (
            boosters.map((item) => (
              <TableRow key={item.id}>
                <TableCell>#{item.id}</TableCell>
                <TableCell className="font-mono text-xs">{item.booster_key}</TableCell>
                <TableCell className="font-medium">{item.booster_name}</TableCell>
                <TableCell>
                  <Badge variant="secondary" className="bg-yellow-500/10 text-yellow-500 border-yellow-500/20">
                    {item.coin_cost} 💰
                  </Badge>
                </TableCell>
                <TableCell className="text-right">
                  <Button variant="ghost" size="icon" className="h-8 w-8">
                    <Pencil className="h-4 w-4" />
                  </Button>
                  <Button variant="ghost" size="icon" className="h-8 w-8 text-destructive" onClick={() => handleDelete(item.id)}>
                    <Trash2 className="h-4 w-4" />
                  </Button>
                </TableCell>
              </TableRow>
            ))
          )}
        </TableBody>
      </Table>
    </div>
  )
}