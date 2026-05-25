import * as React from "react";
import { cn } from "../../lib/utils";

export function Badge({ className, ...props }: React.HTMLAttributes<HTMLSpanElement>) {
  return (
    <span
      className={cn(
        "inline-flex h-5 items-center rounded-sm border border-border px-1.5 font-mono text-[11px] font-medium",
        className
      )}
      {...props}
    />
  );
}
