"use client"

import { useState } from "react"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Play, RefreshCw } from "lucide-react"

interface ETLLog {
  id: string
  status: "success" | "failed"
  rowsImported: number
  time: string
  duration: string
}

const initialLogs: ETLLog[] = [
  { id: "ETL-001", status: "success", rowsImported: 45230, time: "2024-01-15 14:30:00", duration: "2m 34s" },
  { id: "ETL-002", status: "success", rowsImported: 38420, time: "2024-01-15 13:30:00", duration: "2m 12s" },
  { id: "ETL-003", status: "failed", rowsImported: 0, time: "2024-01-15 12:30:00", duration: "0m 45s" },
  { id: "ETL-004", status: "success", rowsImported: 52150, time: "2024-01-15 11:30:00", duration: "3m 01s" },
  { id: "ETL-005", status: "success", rowsImported: 41800, time: "2024-01-15 10:30:00", duration: "2m 48s" },
  { id: "ETL-006", status: "success", rowsImported: 39200, time: "2024-01-15 09:30:00", duration: "2m 22s" },
  { id: "ETL-007", status: "failed", rowsImported: 0, time: "2024-01-15 08:30:00", duration: "1m 15s" },
  { id: "ETL-008", status: "success", rowsImported: 47600, time: "2024-01-15 07:30:00", duration: "2m 56s" },
]

export function ETLLogsTable() {
  const [logs, setLogs] = useState(initialLogs)
  const [isRunning, setIsRunning] = useState(false)

  const formatToVN = (timeStr: string) => {
    if (!timeStr) return ''
    // Normalize space separated datetimes to ISO and assume UTC when no timezone provided
    let iso = timeStr
    if (!timeStr.includes('T') && !timeStr.endsWith('Z')) {
      iso = timeStr.replace(' ', 'T') + 'Z'
    }
    const d = new Date(iso)
    if (isNaN(d.getTime())) return timeStr
    return d.toLocaleString('vi-VN', {
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
    })
  }

  const runNow = () => {
    setIsRunning(true)
    setTimeout(() => {
      const newLog: ETLLog = {
        id: `ETL-${String(logs.length + 1).padStart(3, "0")}`,
        status: Math.random() > 0.2 ? "success" : "failed",
        rowsImported: Math.random() > 0.2 ? Math.floor(Math.random() * 20000) + 35000 : 0,
        time: new Date().toISOString().replace("T", " ").substring(0, 19),
        duration: `${Math.floor(Math.random() * 3) + 1}m ${Math.floor(Math.random() * 60)}s`,
      }
      setLogs([newLog, ...logs])
      setIsRunning(false)
    }, 2000)
  }

  return (
    <Card className="border-border bg-card">
      <CardHeader className="flex flex-row items-center justify-between">
        <CardTitle className="text-foreground">ETL Job Logs</CardTitle>
        <Button
          onClick={runNow}
          disabled={isRunning}
          className="bg-primary text-primary-foreground hover:bg-primary/80"
        >
          {isRunning ? (
            <>
              <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
              Running...
            </>
          ) : (
            <>
              <Play className="mr-2 h-4 w-4" />
              Run Now
            </>
          )}
        </Button>
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow className="border-border hover:bg-transparent">
              <TableHead className="text-muted-foreground">Job ID</TableHead>
              <TableHead className="text-muted-foreground">Status</TableHead>
              <TableHead className="text-right text-muted-foreground">Rows Imported</TableHead>
              <TableHead className="text-muted-foreground">Time</TableHead>
              <TableHead className="text-right text-muted-foreground">Duration</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {logs.map((log) => (
              <TableRow key={log.id} className="border-border">
                <TableCell className="font-mono text-neon-cyan">{log.id}</TableCell>
                <TableCell>
                  <Badge
                    variant="outline"
                    className={
                      log.status === "success"
                        ? "border-neon-green bg-neon-green/10 text-neon-green"
                        : "border-destructive bg-destructive/10 text-destructive"
                    }
                  >
                    {log.status}
                  </Badge>
                </TableCell>
                <TableCell className="text-right text-foreground">{log.rowsImported.toLocaleString()}</TableCell>
                <TableCell className="text-muted-foreground">{formatToVN(log.time)}</TableCell>
                <TableCell className="text-right text-foreground">{log.duration}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  )
}
