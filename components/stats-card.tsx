import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { cn } from "@/lib/utils"
import type { LucideIcon } from "lucide-react"

interface StatsCardProps {
  title: string
  value: string
  change?: string
  changeType?: "positive" | "negative" | "neutral"
  icon: LucideIcon
  accentColor?: "cyan" | "magenta" | "green"
}

export function StatsCard({
  title,
  value,
  change,
  changeType = "neutral",
  icon: Icon,
  accentColor = "cyan",
}: StatsCardProps) {
  return (
    <Card
      className={cn(
        "border-border bg-card transition-all hover:border-primary/50",
        accentColor === "cyan" && "hover:neon-border-cyan",
        accentColor === "magenta" && "hover:neon-border-magenta",
        accentColor === "green" && "hover:neon-border-green",
      )}
    >
      <CardHeader className="flex flex-row items-center justify-between pb-2">
        <CardTitle className="text-sm font-medium text-muted-foreground">{title}</CardTitle>
        <Icon
          className={cn(
            "h-5 w-5",
            accentColor === "cyan" && "text-neon-cyan",
            accentColor === "magenta" && "text-neon-magenta",
            accentColor === "green" && "text-neon-green",
          )}
        />
      </CardHeader>
      <CardContent>
        <div
          className={cn(
            "text-3xl font-bold",
            accentColor === "cyan" && "neon-text-cyan",
            accentColor === "magenta" && "neon-text-magenta",
            accentColor === "green" && "neon-text-green",
          )}
        >
          {value}
        </div>
        {change && (
          <p
            className={cn(
              "mt-1 text-xs",
              changeType === "positive" && "text-neon-green",
              changeType === "negative" && "text-destructive",
              changeType === "neutral" && "text-muted-foreground",
            )}
          >
            {change}
          </p>
        )}
      </CardContent>
    </Card>
  )
}
