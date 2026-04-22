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

import React, { useState, useEffect, useRef } from "react";
import { toast } from "react-toastify";
import SendIcon from "@mui/icons-material/Send";
import CancelIcon from "@mui/icons-material/Cancel";
import DownloadIcon from "@mui/icons-material/Download";
import MicIcon from "@mui/icons-material/Mic";
import ChatBubbleOutlineIcon from "@mui/icons-material/ChatBubbleOutline";
import FormGroup from '@mui/material/FormGroup';
import FormControlLabel from '@mui/material/FormControlLabel';
import Switch from '@mui/material/Switch';
import { styled } from '@mui/material/styles';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faTimesCircle } from '@fortawesome/free-solid-svg-icons';

import ChatMessage from "./ChatMessage";
import { ChatboxProps } from "../../types";
import { config } from "../../config/config";
import { showCartNotification } from "../../utils";
import nvidialogo from "../../assets/nvidia-logo.png";
import arrowlogo from "../../assets/arrow_logo.png";

/**
 * Main chatbox component for the shopping assistant
 */

// Custom styled switch component
const CustomSwitch = styled(Switch)(({ theme }) => ({
  '& .MuiSwitch-switchBase.Mui-checked': {
    color: '#0073ffff',
  },
  '& .MuiSwitch-switchBase.Mui-checked + .MuiSwitch-track': {
    backgroundColor: '#85b9f9ff',
  },
  '& .MuiSwitch-track': {
    backgroundColor: 'lightgray',
  },
}));

const Chatbox: React.FC<ChatboxProps> = ({ setNewRenderImage }) => {
  const [isOpen, setIsOpen] = useState<boolean>(true);
  const [hasBeenOpened, setHasBeenOpened] = useState<boolean>(false);
  const [newMessage, setNewMessage] = useState<string>("");
  const [isGuardrailsOn, setIsGuardrailsOn] = useState(config.features.guardrails.defaultState);
  const [image, setImage] = useState("");
  const [previewImage, setPreviewImage] = useState("");
  const [messages, setMessages] = useState<any[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const messageRefs = useRef<React.RefObject<HTMLDivElement>[]>([]);
  const [lastAssistantIndex, setLastAssistantIndex] = useState<number | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const shownCartOperations = useRef<Set<string>>(new Set());
  const [isRecording, setIsRecording] = useState<boolean>(false);
  const recognitionRef = useRef<SpeechRecognition | null>(null);
  const interimTranscriptRef = useRef<string>("");
  const finalTranscriptRef = useRef<string>("");
  const baseMessageRef = useRef<string>("");

  // Utility functions
  const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

  const getOrCreateUserId = () => {
    const storedId = sessionStorage.getItem('shopping_user_id');
    if (storedId) return parseInt(storedId, 10);
    
    const newId = Date.now() * 1000 + Math.floor(Math.random() * 1000);
    sessionStorage.setItem('shopping_user_id', String(newId));
    return newId;
  };

  const convertToBase64 = (file: File): Promise<string> => {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(reader.result as string);
      reader.onerror = (error) => reject(new Error("Failed to read the file."));
      reader.readAsDataURL(file);
    });
  };

  const base64ToBlob = (base64: string): Blob => {
    const base64WithoutPrefix = base64.split(',')[1];
    const binaryString = atob(base64WithoutPrefix);
    const byteArray = new Uint8Array(binaryString.length);
    
    for (let i = 0; i < binaryString.length; i++) {
      byteArray[i] = binaryString.charCodeAt(i);
    }
    
    return new Blob([byteArray], { type: "image/png" });
  };

  // Event handlers
  const toggleGuardrails = () => {
    setIsGuardrailsOn(!isGuardrailsOn);
  };

  const handleNewMessageChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setNewMessage(event.target.value);
  };

  const handleImageUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = e.target.files;
    if (!files || files.length === 0) return;

    const file = files[0];
    
    // Validate file
    const maxSizeMB = config.features.imageUpload.maxSize;
    if (file.size > maxSizeMB * 1024 * 1024) {
      toast.error(`File size must be less than ${maxSizeMB}MB`);
      return;
    }

    if (!config.features.imageUpload.allowedTypes.includes(file.type)) {
              toast.error('Please select a valid image file (JPEG or PNG only)');
      return;
    }

    try {
      const base64Image = await convertToBase64(file);
      setImage(base64Image);
      
      const decodedImage = base64ToBlob(base64Image);
      const imageUrl = window.URL.createObjectURL(decodedImage);
      setPreviewImage(imageUrl);
      
      e.target.value = "";
    } catch (error) {
      toast.error('Failed to upload image');
    }
  };

  const clearImage = () => {
    setPreviewImage("");
    setImage("");
  };

  const addMessage = (role: string, content: any, productName: string = "") => {
    setMessages((prevMessages) => {
      const newMessages = [...prevMessages, { role, content, productName }];
      messageRefs.current = newMessages.map((_, i) => 
        messageRefs.current[i] || React.createRef<HTMLDivElement>()
      );
      
      if (role === "assistant" && (lastAssistantIndex === null || lastAssistantIndex < prevMessages.length)) {
        setLastAssistantIndex(prevMessages.length);
      }
      
      return newMessages;
    });
  };

  const updateLastMessage = (newContent: any, role?: string, appendContent?: boolean) => {
    setMessages((prevMessages) => {
      if (prevMessages.length === 0) return prevMessages;

      const updatedMessages = [...prevMessages];
      const lastMessageIndex = updatedMessages.length - 1;
      
      if (role) {
        updatedMessages[lastMessageIndex].role = role;
      }
      
      if (typeof newContent === "string") {
        updatedMessages[lastMessageIndex] = {
          ...updatedMessages[lastMessageIndex],
          content: (!appendContent) 
            ? updatedMessages[lastMessageIndex].content + newContent 
            : newContent,
        };
      } else {
        updatedMessages[lastMessageIndex] = {
          ...updatedMessages[lastMessageIndex],
          content: newContent,
        };
      }

      return updatedMessages;
    });
  };

  const handleSendMessage = async () => {
    if (!newMessage.trim() && !image) return;

    // Save the message before clearing (needed for API request)
    const messageToSend = newMessage;

    // Stop recording if active and clear all speech recognition refs
    if (isRecording && recognitionRef.current) {
      recognitionRef.current.stop();
      recognitionRef.current = null;
      setIsRecording(false);
      interimTranscriptRef.current = '';
      finalTranscriptRef.current = '';
      baseMessageRef.current = '';
    }

    // Clear previous cart operation notifications for new message
    shownCartOperations.current.clear();

    const userId = getOrCreateUserId();
    setIsLoading(true);

    // Will be used to enable submit shortly after the last token
    let enableSubmitTimer: number | undefined;

    try {
      // Enable-submit helper: if no tokens arrive for a short window, consider the stream done
      const scheduleEnableSubmit = () => {
        if (enableSubmitTimer !== undefined) {
          window.clearTimeout(enableSubmitTimer);
        }
        // Short idle threshold so the button enables promptly after the last token
        enableSubmitTimer = window.setTimeout(() => {
          setIsLoading(false);
        }, 400);
      };

      // Add user message
      if (messageToSend) {
        addMessage("user", messageToSend, "");
      }
      if (image) {
        addMessage("user_image", previewImage, "");
      }

      // Add loading message
      addMessage("assistant", "loader", "");
      
      // Clear message input immediately and ensure speech recognition refs are cleared
      setNewMessage("");
      // Double-check refs are cleared to prevent speech recognition from restoring text
      interimTranscriptRef.current = '';
      finalTranscriptRef.current = '';
      baseMessageRef.current = '';

      // Prepare API request
      const payload = {
        user_id: userId,
        query: messageToSend,
        guardrails: isGuardrailsOn,
        image: image || "",
        image_bool: !!image
      };
      
      // Clear image immediately after preparing payload
      setImage("");
      setPreviewImage("");

      const url = `${config.api.baseUrl}${config.api.endpoints.stream}`;

      // Send request
      const response = await fetch(url, {
        method: "POST",
        mode: "cors",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      });

      if (!response.ok || !response.body) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      // Process streaming response
      const reader = response.body.getReader();
      const decoder = new TextDecoder('utf-8');
      let fullResponse = "";

      while (true) {
        const { value, done } = await reader.read();
        if (done) break;

        const chunk = decoder.decode(value, { stream: true });
        const lines = chunk.split('\n').filter(line => line.startsWith('data:'));

        for (let line of lines) {
          const raw = line.replace(/^data:\s*/, '');
          
          if (raw === '[DONE]') {
            // Stream closed by server; enable submit immediately
            setIsLoading(false);
            return;
          }
          
          try {
            const { type, payload } = JSON.parse(raw);
            
            if (type === 'content') {
              fullResponse += payload;
              
              // Check for cart operations and show notifications
              showCartNotification(fullResponse, shownCartOperations.current, toast);

              // Tokens are flowing; schedule enable when they stop
              scheduleEnableSubmit();
            } else if (type === 'images') {
              const images = Object.entries(payload).map(([productName, productUrl]) => ({ 
                productUrl, 
                productName 
              }));
              
              setMessages(prev => {
                const updated = [...prev];
                updated[updated.length - 1] = {
                  ...updated[updated.length - 1],
                  role: 'image_row',
                  content: images,
                };
                return updated;
              });
            }

            // Update assistant message
            setMessages(prev => {
              const updated = [...prev];
              const last = updated[updated.length - 1];

              if (last?.role === 'assistant') {
                updated[updated.length - 1] = {
                  ...last,
                  content: fullResponse
                };
              } else {
                updated.push({
                  role: 'assistant',
                  content: fullResponse,
                  productName: ""
                });
                setLastAssistantIndex(updated.length - 1);
              }

              return updated;
            });
          } catch (e) {
            continue;
          }
        }
      }
      
    } catch (error) {
      console.error('Error sending message:', error);
      toast.error('Failed to send message. Please try again.');
      
      // Remove loading message on error
      setMessages(prev => prev.filter(msg => msg.content !== 'loader'));
    } finally {
      // Clear any pending enable timer and ensure loading is false
      if (enableSubmitTimer !== undefined) window.clearTimeout(enableSubmitTimer);
      setIsLoading(false);
    }
  };

  const handleKeyUp = (event: React.KeyboardEvent<HTMLInputElement>) => {
    if (event.key === "Enter" && !isLoading) {
      handleSendMessage();
    }
  };

  const initializeSpeechRecognition = () => {
    if (typeof window === 'undefined') return null;
    
    // Speech Recognition requires HTTPS or localhost (secure context)
    if (!window.isSecureContext) {
      toast.error('Voice input requires a secure connection (HTTPS). Please open this app via HTTPS.');
      return null;
    }
    
    const SpeechRecognition = window.SpeechRecognition || (window as any).webkitSpeechRecognition;
    if (!SpeechRecognition) {
      toast.error('Speech recognition is not supported in your browser. Try Chrome or Edge.');
      return null;
    }

    const recognition = new SpeechRecognition();
    recognition.continuous = true;
    recognition.interimResults = true;
    recognition.lang = 'en-US';

    recognition.onstart = () => {
      setIsRecording(true);
    };

    recognition.onresult = (event: SpeechRecognitionEvent) => {
      // Don't update if recording was stopped (recognitionRef is null)
      if (!recognitionRef.current) {
        return;
      }

      // Process all results from the current index
      let newFinalTranscript = '';
      let newInterimTranscript = '';

      for (let i = event.resultIndex; i < event.results.length; i++) {
        const result = event.results[i];
        if (!result.length) continue;
        const transcript = result[0].transcript;
        if (result.isFinal) {
          // Add to final transcript (permanent)
          newFinalTranscript += transcript + ' ';
          finalTranscriptRef.current += transcript + ' ';
          // Clear interim when we get a final result
          interimTranscriptRef.current = '';
        } else {
          // This is interim (temporary)
          newInterimTranscript += transcript;
        }
      }

      // Update interim transcript ref
      if (newInterimTranscript) {
        interimTranscriptRef.current = newInterimTranscript;
      }

      // Build message: base (from before recording) + all final transcripts + current interim
      // Only update if we're still recording
      if (recognitionRef.current) {
        setNewMessage(baseMessageRef.current + finalTranscriptRef.current + interimTranscriptRef.current);
      }
    };

    recognition.onerror = (event: SpeechRecognitionErrorEvent) => {
      console.error('Speech recognition error:', event.error);
      if (event.error === 'no-speech') {
        // This is common when user stops speaking, don't show error
        return;
      }
      if (event.error === 'not-allowed') {
        toast.error('Microphone permission denied. Please allow microphone access.');
      } else if (event.error === 'network') {
        toast.error('Network error occurred during speech recognition.');
      } else {
        toast.error(`Speech recognition error: ${event.error}`);
      }
      setIsRecording(false);
    };

    recognition.onend = () => {
      setIsRecording(false);
      // Clear interim but keep final transcripts
      interimTranscriptRef.current = '';
    };

    return recognition;
  };

  const toggleSpeechRecognition = () => {
    if (isRecording) {
      // Stop recording
      if (recognitionRef.current) {
        recognitionRef.current.stop();
        recognitionRef.current = null;
      }
      setIsRecording(false);
      interimTranscriptRef.current = '';
      finalTranscriptRef.current = '';
      baseMessageRef.current = '';
    } else {
      // Start recording - save the current message as base text
      baseMessageRef.current = newMessage;
      finalTranscriptRef.current = '';
      interimTranscriptRef.current = '';
      
      // Start recording
      const recognition = initializeSpeechRecognition();
      if (recognition) {
        recognitionRef.current = recognition;
        try {
          recognition.start();
        } catch (error) {
          console.error('Error starting speech recognition:', error);
          toast.error('Failed to start speech recognition');
          setIsRecording(false);
        }
      }
    }
  };

  const handleReset = async () => {
    // Stop recording if active
    if (isRecording && recognitionRef.current) {
      recognitionRef.current.stop();
      recognitionRef.current = null;
      setIsRecording(false);
      interimTranscriptRef.current = '';
      finalTranscriptRef.current = '';
      baseMessageRef.current = '';
    }
    
    setMessages([]);
    setImage("");
    setPreviewImage("");
    setNewRenderImage("");
    sessionStorage.removeItem('shopping_user_id');

    // Add welcome messages
    addMessage(
      "system",
      "You are the Arrow Electronics Assistant. You help answer questions for customers about electronics and technology products. Start the conversation by asking a couple of questions to clarify what the user is looking for. Use emojis but do not use too many. Structure your output using Markdown but do not use nested indentations.",
      ""
    );
    
    await sleep(1000);
    addMessage("assistant", "", "");
    
    await sleep(1000);
    const introduction = "Hello! 👋 I'm your Arrow Electronics Assistant, here on Arrow's AI Experience Center. You can ask me anything—from finding the right part to learning more about product features.\n\nHere are some example questions you could ask me:\n\n• I need a plug-in power supply—like a 12V wall adapter for a small device. What do you have?\n• I'm looking for a small Wi-Fi or microcontroller kit for a hobby project—what would you suggest?\n• I have a photo of a circuit board or part—can you find something like it? (add a picture)\n• Please add the 12V power adapter to my cart.\n• What's in my cart, and what's my total?";
    
    const words = introduction.split(" ");
    for (const word of words) {
      await sleep(40);
      updateLastMessage(word + " ");
    }
  };

  // Effects
  useEffect(() => {
    if (lastAssistantIndex !== null) {
      const messageRef = messageRefs.current[lastAssistantIndex];
      if (messageRef && messageRef.current) {
        messageRef.current.scrollIntoView({ behavior: "smooth", block: "start" });
      }
    }
    if (!isLoading) {
      inputRef.current?.focus();
    }
  }, [messages, isLoading]);

  useEffect(() => {
    if (isOpen) {
      setHasBeenOpened(true);
    }
  }, [isOpen]);

  useEffect(() => {
    if (hasBeenOpened) {
      handleReset();
    }
  }, [hasBeenOpened]);

  // Cleanup: stop recording when component unmounts
  useEffect(() => {
    return () => {
      if (recognitionRef.current) {
        recognitionRef.current.stop();
        recognitionRef.current = null;
      }
    };
  }, []);

  return (
    <div>
      <div className="chatbox">
        <div className={`chatbox__support ${isOpen ? "chatbox--active" : ""}`}>
          {/* Header */}
          <div className="chatbox__header" style={{ 
            display: 'flex', 
            justifyContent: 'center', 
            alignItems: 'center',
            padding: '15px 20px',
            position: 'relative'
          }}>
            <h4 className="chatbox__heading--header" style={{ margin: 0, textAlign: 'center', width: '100%' }}>
              Arrow Electronics Technology Assistant
            </h4>
          </div>

          {/* Messages */}
          <div className="chatbox__messages">
            {[...messages].reverse().map((msg, index) => (
              <ChatMessage 
                key={index} 
                role={msg.role} 
                content={msg.content} 
                productName={msg.productName} 
                ref={messageRefs.current[messages.length - 1 - index]} 
              />
            ))}
          </div>

          {/* Footer */}
          <div className="chatbox__footer">
            {/* Image preview */}
            {previewImage && (
              <div style={{ 
                position: 'relative', 
                display: 'inline-block',
                marginBottom: '8px',
                width: '100%'
              }}>
                <img 
                  src={previewImage} 
                  alt="Preview" 
                  style={{ 
                    width: '60px', 
                    height: '60px',
                    borderRadius: '8px',
                    objectFit: 'cover'
                  }} 
                />
                <button
                  type="button"
                  style={{
                    display: 'inline-flex',
                    position: 'absolute',
                    right: '-5px',
                    top: '-5px',
                    cursor: 'pointer',
                    background: 'white',
                    border: 'none',
                    padding: '2px',
                    borderRadius: '50%',
                    boxShadow: '0 2px 4px rgba(0,0,0,0.2)'
                  }}
                  onClick={clearImage}
                  aria-label="Clear image"
                >
                  <FontAwesomeIcon icon={faTimesCircle} style={{ color: '#ff0000', fontSize: '18px' }} />
                </button>
              </div>
            )}

            {/* Input and buttons container */}
            <div style={{ 
              display: 'flex', 
              alignItems: 'center', 
              gap: '8px', 
              width: '100%',
              flexWrap: previewImage ? 'wrap' : 'nowrap'
            }}>
              {/* Input field */}
              <input
                ref={inputRef}
                type="text"
                className="input_test"
                placeholder="Type something here..."
                value={newMessage}
                onChange={handleNewMessageChange}
                onKeyUp={handleKeyUp}
                style={{ flex: '1', minWidth: '0' }}
              />

              {/* Action buttons */}
              <div style={{ display: 'flex', gap: '4px', alignItems: 'center' }}>
                <div className="button-class">
                  <SendIcon
                    sx={{ 
                      color: isLoading ? "lightgray" : "#0073ffff", 
                      cursor: isLoading ? "not-allowed" : "pointer",
                      fontSize: { xs: "24px", md: "28px" }
                    }}
                    onClick={isLoading ? () => {} : handleSendMessage}
                  />
                </div>
                
                <div className="button-class">
                  <MicIcon
                    sx={{ 
                      color: isRecording ? "#ff0000" : "#0073ffff", 
                      cursor: "pointer",
                      fontSize: { xs: "24px", md: "28px" }
                    }}
                    onClick={toggleSpeechRecognition}
                  />
                </div>
                
                <div className="button-class">
                  <CancelIcon
                    sx={{ 
                      color: "#0073ffff",
                      fontSize: { xs: "24px", md: "28px" }
                    }}
                    onClick={handleReset}
                  />
                </div>
                
                <div className="button-class" style={{ transform: "rotate(180deg)" }}>
                  <label htmlFor="image-upload" style={{ cursor: "pointer", display: "flex", alignItems: "center" }}>
                    <DownloadIcon
                      sx={{ 
                        color: "#0073ffff",
                        fontSize: { xs: "24px", md: "28px" }
                      }}
                    />
                  </label>
                  <input
                    style={{ display: "none" }}
                    type="file"
                    accept="image/*"
                    id="image-upload"
                    name="image"
                    onChange={handleImageUpload}
                  />
                </div>
              </div>
            </div>
          </div>

          {/* Guardrails toggle */}
          <div className="chatbox__guardrail">
            <FormGroup>
              <FormControlLabel 
                control={
                  <CustomSwitch 
                    checked={isGuardrailsOn}
                    onChange={toggleGuardrails}
                  />
                } 
                label="Guardrails" 
              />
            </FormGroup>
          </div>

          {/* Powered by NVIDIA */}
          <div className="flex relative flex-row items-center justify-center bg-white pb-[15px] pt-[10px]" style={{ flexWrap: 'wrap', gap: '4px' }}>
            <h3 className="text-[16px]" style={{ fontSize: 'clamp(10px, 2.5vw, 16px)' }}>Powered by</h3>
            <img src={nvidialogo} alt="NVIDIA" className="h-14" style={{ height: 'clamp(28px, 7vw, 56px)', width: 'auto' }} />
            <h3 className="text-[16px]" style={{ fontWeight: "bold", marginRight: "12px", marginLeft: "12px", fontSize: 'clamp(10px, 2.5vw, 16px)' }}> x </h3>
            <img src={arrowlogo} alt="Arrow Electronics ECS" style={{width: "clamp(50px, 12vw, 100px)", height: "auto"}} />
          </div>
        </div>

        {/* Chatbox toggle button (hidden) */}
        <div className="chatbox__button" style={{ visibility: "hidden" }}>
          <button type="button" onClick={() => setIsOpen(!isOpen)} aria-label="Open chat">
            <ChatBubbleOutlineIcon fontSize="large" />
          </button>
        </div>
      </div>
    </div>
  );
};

export default Chatbox;
