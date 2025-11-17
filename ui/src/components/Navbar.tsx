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
 * Navigation bar component
 */

import React from "react";
import MenuIcon from "@mui/icons-material/Menu";
import { config, isHardwareMode } from "../config/config";
import logo from "../assets/CDW-Logo.png";

const Navbar: React.FC = () => {
  const categories = config.ui.categories;

  const getCategoryLink = (categoryKey: keyof typeof categories): string => {
    // Remove all mode switching logic
    return "#";
  };

  const isCategoryActive = (categoryKey: keyof typeof categories): boolean => {
    // Only hardware is active
    return categoryKey === 'hardware';
  };

  return (
    <div>
      {/* Main navigation bar */}
      <div className="bg-[#FFFFFF] h-[60px] px-3 py-2 lg:px-5 text-white flex justify-between items-center">
        {/* Left side - Menu and Brand */}
        <div className="flex items-center shrink-0">
          <MenuIcon sx={{ color: "#5E5E5E" }} fontSize="small"/>
          {/* <p className="text-[22px] ml-[20px] font-bold text-[#202020]">
            CDW
          </p> */}
          <img src={logo} alt="logo" className="h-8 md:h-12 w-auto" style={{ paddingLeft: "12px" }}/>
          
        </div>
        
        {/* Right side - Welcome message */}
        <div className="flex items-center gap-x-2">
          <div className="flex items-center gap-2 p-3 rounded-full">
            <p className="text-[14px] text-[#202020]">Welcome!</p>
          </div>
        </div>
      </div>

      {/* Categories bar */}
      <div className="bg-[#F2F2F2] mt-[1px] h-[57px] text-white px-3 py-2 lg:px-8 flex items-center gap-2 md:gap-4 overflow-x-auto whitespace-nowrap scrollbar-hide">
        {/* Hardware - Always Active */}
        <div className="flex items-center">
          <p className="text-[15px] font-medium text-[#000] underline">
            {categories.hardware}
          </p>
        </div>

        {/* Software */}
        <div className="flex items-center hover:underline">
          <p className="text-[15px] text-[#666] font-medium hover:underline">
            {categories.software}
          </p>
        </div>

        {/* Solutions */}
        <div className="flex items-center hover:underline">
          <p className="text-[15px] text-[#666] font-medium hover:underline">
            {categories.solutions}
          </p>
        </div>

        {/* Services */}
        <div className="flex items-center hover:underline">
          <p className="text-[15px] text-[#666] font-medium hover:underline">
            {categories.services}
          </p>
        </div>

        {/* Industries */}
        <div className="flex items-center hover:underline">
          <p className="text-[15px] text-[#666] font-medium hover:underline">
            {categories.industries}
          </p>
        </div>

        {/* Partners */}
        <div className="flex items-center hover:underline">
          <p className="text-[15px] text-[#666] font-medium hover:underline">
            {categories.partners}
          </p>
        </div>

        {/* Insights */}
        <div className="flex items-center hover:underline">
          <p className="text-[15px] text-[#666] font-medium hover:underline">
            {categories.insights}
          </p>
        </div>

        {/* Why CDW */}
        <div className="flex items-center hover:underline">
          <p className="text-[15px] text-[#666] font-medium hover:underline">
            {categories.why}
          </p>
        </div>
      </div>
    </div>
  );
};

export default Navbar;
