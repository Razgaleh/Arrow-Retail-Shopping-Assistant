// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * Configuration file for the Shopping Assistant UI
 */

export interface AppConfig {
  api: {
    baseUrl: string;
    port: number;
    endpoints: {
      query: string;
      stream: string;
      live: string;
      health: string;
    };
  };
  ui: {
    defaultImages: {
      hardware: string;
    };
    categories: {
      electronicComponents: string;
      products: string;
      manufacturers: string;
      datasheets: string;
      referenceDesigns: string;
      verticalsTrends: string;
      bomTool: string;
    };
  };
  features: {
    guardrails: {
      enabled: boolean;
      defaultState: boolean;
    };
    imageUpload: {
      enabled: boolean;
      maxSize: number; // in MB
      allowedTypes: string[];
    };
  };
}

// Get configuration based on environment
const getConfig = (): AppConfig => {
  // Always use nginx proxy - it handles the routing
  const baseUrl = '/api';

  return {
    api: {
      baseUrl: baseUrl,
      port: 80,
      endpoints: {
        query: '/query',
        stream: '/query/stream',
        live: '/query/live',
        health: '/health',
      },
    },
    ui: {
      defaultImages: {
        hardware: "/images/splash.jpg",
      },
      categories: {
        electronicComponents: "Electronic Components",
        products: "Products",
        manufacturers: "Manufacturers",
        datasheets: "Datasheets",
        referenceDesigns: "Reference Designs",
        verticalsTrends: "Verticals & Trends",
        bomTool: "BOM Tool"
      }
    },
    features: {
      guardrails: {
        enabled: true,
        defaultState: true,
      },
      imageUpload: {
        enabled: true,
        maxSize: 10, // 10MB
        allowedTypes: ['image/jpeg', 'image/png'],
      },
    },
  };
};

export const config = getConfig();

// Helper functions
export const getApiUrl = (endpoint: keyof AppConfig['api']['endpoints']): string => {
  return `${config.api.baseUrl}${config.api.endpoints[endpoint]}`;
};

export const isHardwareMode = (): boolean => {
  return true; // Always return true - always fashion mode
};

export const getDefaultImage = (): string => {
  return config.ui.defaultImages.hardware; // Always use fashion image
}; 