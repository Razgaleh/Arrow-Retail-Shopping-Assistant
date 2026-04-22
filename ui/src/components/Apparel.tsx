/*
 * SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Large panel: product image from chat, or branded hero when idle.
 * Palette inspired by Arrow-style marketing: warm orange accent on charcoal / cool gray.
 */

import React from "react";
import MicIcon from "@mui/icons-material/Mic";
import { ApparelProps } from "../types";

const HERO_TAGS = ["Components", "Power & supplies", "RF & wireless", "Dev kits", "Sensors & tools"];

/** Arrow-adjacent marketing accents (not official brand tokens; visually aligned with arrow.com CTAs / highlights). */
const brand = {
  orange: "#E87722",
  orangeDeep: "#C45F12",
  orangeSoft: "rgba(232, 119, 34, 0.22)",
  orangeGlow: "rgba(245, 166, 80, 0.35)",
  charcoal: "#1c1c1e",
  charcoalMid: "#2d2d32",
  mist: "#F2F2F2",
};

const Apparel: React.FC<ApparelProps> = ({ newRenderImage }) => {
  if (newRenderImage) {
    return (
      <div
        className="hidden md:flex overflow-hidden items-center justify-center h-[85vh] flex-grow-1 object-contain"
        style={{ width: "40vw" }}
      >
        <img src={newRenderImage} alt="Product from your conversation" className="product-image" />
      </div>
    );
  }

  return (
    <div
      className="hidden md:flex h-[85vh] flex-grow-1 items-stretch justify-center p-4 box-border"
      style={{ width: "40vw", minWidth: "min(40vw, 420px)" }}
    >
      <div
        className="relative flex flex-1 flex-col justify-between overflow-hidden rounded-2xl border shadow-lg"
        style={{
          maxHeight: "100%",
          borderColor: "rgba(255,255,255,0.12)",
          background: `linear-gradient(145deg, ${brand.charcoal} 0%, ${brand.charcoalMid} 45%, #252528 100%)`,
        }}
      >
        <div
          className="pointer-events-none absolute inset-0"
          style={{
            opacity: 0.5,
            backgroundImage: `radial-gradient(ellipse 90% 70% at 85% 15%, ${brand.orangeGlow} 0%, transparent 55%),
              radial-gradient(ellipse 60% 50% at 10% 85%, ${brand.orangeSoft} 0%, transparent 50%),
              repeating-linear-gradient(-14deg, transparent, transparent 36px, rgba(255,255,255,0.025) 36px, rgba(255,255,255,0.025) 37px)`,
          }}
        />
        <div className="relative z-10 flex flex-1 flex-col justify-center px-8 py-8 md:px-10 md:py-10">
          <p
            className="text-xs font-semibold uppercase tracking-[0.18em]"
            style={{ color: brand.orange }}
          >
            Arrow Electronics Assistant
          </p>
          <h2 className="mt-3 text-2xl font-semibold leading-tight text-white md:text-3xl">
            Find the right part, faster
          </h2>
          <p className="mt-4 max-w-md text-sm leading-relaxed md:text-base" style={{ color: brand.mist }}>
            Describe what you need, share a photo of a board or component, or ask about specs and
            availability — results appear here as you chat.
          </p>

          {/* Voice assistant highlight */}
          <div
            className="mt-6 flex max-w-[45%] gap-3 self-start rounded-xl border px-3 py-3 backdrop-blur-sm"
            style={{
              borderColor: "rgba(232, 119, 34, 0.45)",
              background: "linear-gradient(90deg, rgba(232, 119, 34, 0.14) 0%, rgba(28, 28, 30, 0.65) 100%)",
            }}
          >
            <div
              className="flex h-9 w-9 shrink-0 items-center justify-center rounded-full self-start"
              style={{ backgroundColor: brand.orange, color: "#fff" }}
              aria-hidden
            >
              <MicIcon sx={{ fontSize: 20 }} />
            </div>
            <div className="min-w-0 flex items-center">
              <p className="text-xs font-medium text-white">Talk to the AI Assistant</p>
            </div>
          </div>

          <ul className="mt-6 space-y-2 text-sm" style={{ color: "rgba(242, 242, 242, 0.72)" }}>
            <li className="flex items-center gap-2">
              <span className="h-1.5 w-1.5 shrink-0 rounded-full" style={{ backgroundColor: brand.orange }} />
              Natural-language &amp; image search
            </li>
            <li className="flex items-center gap-2">
              <span className="h-1.5 w-1.5 shrink-0 rounded-full" style={{ backgroundColor: brand.orange }} />
              Cart &amp; product context in one flow
            </li>
          </ul>
          <div className="mt-6 flex flex-wrap gap-2">
            {HERO_TAGS.map((tag) => (
              <span
                key={tag}
                className="rounded-full border px-3 py-1 text-xs font-medium backdrop-blur-sm"
                style={{
                  borderColor: "rgba(232, 119, 34, 0.35)",
                  backgroundColor: "rgba(255, 255, 255, 0.06)",
                  color: brand.mist,
                }}
              >
                {tag}
              </span>
            ))}
          </div>
        </div>
        <div
          className="relative z-10 border-t px-8 py-4 md:px-10"
          style={{
            borderColor: "rgba(255,255,255,0.1)",
            backgroundColor: "rgba(0,0,0,0.25)",
          }}
        >
          <p className="text-xs" style={{ color: "rgba(242, 242, 242, 0.5)" }}>
            Tip: try a 12V adapter, a Wi‑Fi dev kit, or &ldquo;what&rsquo;s in my cart?&rdquo;
          </p>
          <p
            className="mt-2 text-[10px] leading-snug sm:text-[11px]"
            style={{ color: "rgba(242, 242, 242, 0.45)" }}
          >
            Use the microphone in the chat bar to ask questions hands-free. Works best in Chrome.
          </p>
        </div>
      </div>
    </div>
  );
};

export default Apparel;
