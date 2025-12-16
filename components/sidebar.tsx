"use client"

import Link from "next/link"
import { usePathname } from "next/navigation"
import { cn } from "@/lib/utils"
import { LayoutDashboard, Activity, Settings, Gamepad2 } from "lucide-react"
import { ModeToggle } from "@/components/mode-toggle"

const routes = [
  { label: "Dashboard", icon: LayoutDashboard, href: "/", color: "text-sky-500" },
  { label: "Monitor", icon: Activity, href: "/monitor", color: "text-violet-500" },
  { label: "Settings", icon: Settings, href: "/settings", color: "text-pink-700" },
]

export function Sidebar() {
  const pathname = usePathname()

  return (
    // Class quan trọng: fixed inset-y-0 left-0 (Ghim chặt lề trái)
    <div className="fixed inset-y-0 left-0 z-50 w-64 space-y-4 py-4 flex flex-col h-full bg-[#111827] text-white border-r border-[#1f2937]">
      <div className="px-3 py-2 flex-1">
        <div className="mb-10 px-4">
          <Link href="/" className="flex items-center pl-2 mb-4">
            <div className="relative h-8 w-8 mr-3">
              <Gamepad2 className="h-8 w-8 text-cyan-400" />
            </div>
            <h1 className="text-2xl font-bold bg-gradient-to-r from-cyan-400 to-purple-600 text-transparent bg-clip-text">
              GameStats
            </h1>
          </Link>
          <div className="pl-2 flex items-center justify-between bg-zinc-900/50 p-2 rounded-lg border border-zinc-800">
            <span className="text-xs text-zinc-400 font-semibold tracking-wider">APPEARANCE</span>
            <ModeToggle />
          </div>
        </div>
        <div className="space-y-1">
          {routes.map((route) => (
            <Link
              key={route.href}
              href={route.href}
              className={cn(
                "text-sm group flex p-3 w-full justify-start font-medium cursor-pointer hover:text-white hover:bg-white/10 rounded-lg transition",
                pathname === route.href ? "text-white bg-white/10" : "text-zinc-400"
              )}
            >
              <div className="flex items-center flex-1">
                <route.icon className={cn("h-5 w-5 mr-3", route.color)} />
                {route.label}
              </div>
            </Link>
          ))}
        </div>
      </div>
      <div className="px-3 py-2">
        <div className="text-xs text-center text-zinc-500">v1.0.4 - Restore</div>
      </div>
    </div>
  )
}