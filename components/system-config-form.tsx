"use client"

import { useState } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Save, Check } from "lucide-react"

export function SystemConfigForm() {
  const [config, setConfig] = useState({
    oracleIp: "192.168.1.100",
    oraclePort: "1521",
    schedule: "0 * * * *",
  })
  const [saved, setSaved] = useState(false)

  const handleSave = () => {
    setSaved(true)
    setTimeout(() => setSaved(false), 2000)
  }

  return (
    <Card className="border-border bg-card">
      <CardHeader>
        <CardTitle className="text-foreground">System Configuration</CardTitle>
      </CardHeader>
      <CardContent className="space-y-6">
        <div className="grid gap-4 md:grid-cols-2">
          <div className="space-y-2">
            <Label htmlFor="oracle-ip" className="text-foreground">
              Oracle IP Address
            </Label>
            <Input
              id="oracle-ip"
              value={config.oracleIp}
              onChange={(e) => setConfig({ ...config, oracleIp: e.target.value })}
              className="border-border bg-secondary text-foreground"
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="oracle-port" className="text-foreground">
              Oracle Port
            </Label>
            <Input
              id="oracle-port"
              value={config.oraclePort}
              onChange={(e) => setConfig({ ...config, oraclePort: e.target.value })}
              className="border-border bg-secondary text-foreground"
            />
          </div>
        </div>
        <div className="space-y-2">
          <Label htmlFor="schedule" className="text-foreground">
            ETL Schedule (Cron)
          </Label>
          <Input
            id="schedule"
            value={config.schedule}
            onChange={(e) => setConfig({ ...config, schedule: e.target.value })}
            placeholder="0 * * * *"
            className="border-border bg-secondary font-mono text-foreground"
          />
          <p className="text-xs text-muted-foreground">Current: Runs every hour at minute 0</p>
        </div>
        <Button onClick={handleSave} className="bg-primary text-primary-foreground hover:bg-primary/80">
          {saved ? (
            <>
              <Check className="mr-2 h-4 w-4" />
              Saved!
            </>
          ) : (
            <>
              <Save className="mr-2 h-4 w-4" />
              Save Configuration
            </>
          )}
        </Button>
      </CardContent>
    </Card>
  )
}
