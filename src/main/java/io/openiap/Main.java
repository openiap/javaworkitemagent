package io.openiap;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class Main {
    // Default workitem queue
    private static final String DEFAULT_WIQ = "javaqueue";
    private static Client client;
    // Add a shutdown latch to keep the main thread alive
    private static final CountDownLatch shutdownLatch = new CountDownLatch(1);

    public static void main(String[] args) {
        try {
            String libpath = NativeLoader.loadLibrary("openiap");
            client = new Client(libpath);
            client.enableTracing("openiap=info", "");

            client.start();
            client.connect("");

            client.onClientEventAsync((event) -> {
                if ("SignedIn".equals(event.event)) {
                    try {
                        onConnected();
                    } catch (Exception e) {
                        client.error("Error in onConnected: " + e.getMessage());
                    }
                }
            });

            // Add shutdown hook to handle graceful termination
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                client.info("Shutting down application...");
                if (client != null) {
                    try {
                        client.close();
                    } catch (Exception e) {
                        client.error("Error closing client: " + e.getMessage());
                    }
                }
                shutdownLatch.countDown();
            }));
            
            // Wait until shutdown is triggered
            client.info("Application started and waiting for workitems...");
            shutdownLatch.await();

        } catch (Exception e) {
            if (client != null) {
                client.error("Error in main: " + e.getMessage());
            } else {
                System.err.println("Error before client initialization: " + e.getMessage());
            }
        } finally {
            // Ensure resources are properly released
            if (client != null) {
                try {
                    client.close();
                } catch (Exception e) {
                    client.error("Error closing client: " + e.getMessage());
                }
            }
        }
    }

    private static void onConnected() throws Exception {
        try {
            String wiq = System.getenv("wiq");
            if (wiq == null || wiq.isEmpty()) {
                wiq = DEFAULT_WIQ;
            }

            String queue = System.getenv("queue");
            if (queue == null || queue.isEmpty()) {
                queue = wiq;
            }

            List<String> originalFiles = getFilesInCurrentDirectory();
            
            String finalWiq = wiq; // Need final for lambda
            client.registerQueueAsync(new RegisterQueueParameters.Builder()
                .queuename(queue)
                .build(), (eventPtr) -> {
                    try {
                        client.info(finalWiq + " received message notification, processing...");
                        
                        // Spawn a thread to handle popWorkitem since registerQueueAsync callback is blocking
                        new Thread(() -> {
                            try {
                                PopWorkitem popRequest = new PopWorkitem.Builder(finalWiq).build();
                                client.info("Popping workitem from " + finalWiq + " in worker thread");
                                Workitem workitem = client.popWorkitem(Workitem.class, popRequest, null);
                                    
                                if (workitem != null) {
                                    processWorkitemWrapper(originalFiles, workitem);
                                    cleanupFiles(originalFiles);
                                } else {
                                    client.info("No workitem found in " + finalWiq + " (someone else might have picked it up)");
                                }
                            } catch (Exception e) {
                                client.error("Error in worker thread: " + e.getMessage());
                            } finally {
                                cleanupFiles(originalFiles);
                            }
                        }).start();
                        
                        // Return immediately from the callback to avoid blocking
                        return "{}";
                    } catch (Exception e) {
                        client.error("Error processing queue message: " + e.getMessage());
                    } finally {
                        cleanupFiles(originalFiles);
                    }
                    
                    // Return empty JSON to acknowledge we processed the queue notification
                    return "{}";
                });
                
            client.info("Consuming queue: " + queue);
            
            // Keep the handler registered by not exiting onConnected()
            // This ensures we keep receiving notifications about new workitems
        } catch (Exception e) {
            client.error("Error in onConnected: " + e.getMessage());
        }
    }

    private static void processWorkitem(Workitem workitem) throws Exception {
        client.info("Processing workitem id " + workitem.id + ", retry #" + workitem.retries);
        
        if (workitem.payload == null) {
            workitem.payload = "{}";
        }
        
        // Update payload with new name
        workitem.payload = workitem.payload.replaceAll("\\}$", ", \"name\": \"Hello kitty\"}");
        if (workitem.payload.equals("{, \"name\": \"Hello kitty\"}")) {
            workitem.payload = "{\"name\": \"Hello kitty\"}";
        }
        
        workitem.name = "Hello kitty";
        
        // Write to a file
        Files.write(Paths.get("hello.txt"), "Hello kitty".getBytes());
    }

    private static void processWorkitemWrapper(List<String> originalFiles, Workitem workitem) {
        try {
            processWorkitem(workitem);
            workitem.state = "successful";
        } catch (Exception e) {
            workitem.state = "retry";
            workitem.errortype = "application"; // Retryable error
            workitem.errormessage = e.getMessage();
            workitem.errorsource = getStackTrace(e);
            client.error(e.getMessage());
        }
        
        try {
            List<String> currentFiles = getFilesInCurrentDirectory();
            List<String> filesAdded = currentFiles.stream()
                .filter(file -> !originalFiles.contains(file))
                .collect(Collectors.toList());
            
            if (!filesAdded.isEmpty()) {
                UpdateWorkitem.Builder builder = new UpdateWorkitem.Builder(workitem);
                // Fixed method call - passing List<String> directly instead of converting to array
                builder.files(filesAdded);
                UpdateWorkitem updateRequest = builder.build();
                client.updateWorkitem(Workitem.class, updateRequest);
            } else {
                UpdateWorkitem.Builder builder = new UpdateWorkitem.Builder(workitem);
                UpdateWorkitem updateRequest = builder.build();
                client.updateWorkitem(Workitem.class, updateRequest);
            }
        } catch (Exception e) {
            client.error("Error updating workitem: " + e.getMessage());
        }
    }

    private static List<String> getFilesInCurrentDirectory() {
        File directory = new File(".");
        File[] files = directory.listFiles();
        List<String> fileList = new ArrayList<>();
        
        if (files != null) {
            fileList = Arrays.stream(files)
                .filter(File::isFile)
                .map(File::getName)
                .collect(Collectors.toList());
        }
        
        return fileList;
    }

    private static void cleanupFiles(List<String> originalFiles) {
        try {
            List<String> currentFiles = getFilesInCurrentDirectory();
            List<String> filesToDelete = currentFiles.stream()
                .filter(file -> !originalFiles.contains(file))
                .collect(Collectors.toList());
            
            for (String file : filesToDelete) {
                Files.deleteIfExists(Paths.get(file));
            }
        } catch (IOException e) {
            client.error("Error cleaning up files: " + e.getMessage());
        }
    }

    private static String getStackTrace(Exception e) {
        StringBuilder stackTrace = new StringBuilder();
        for (StackTraceElement element : e.getStackTrace()) {
            stackTrace.append(element.toString()).append("\n");
        }
        return stackTrace.length() > 0 ? stackTrace.toString() : "Unknown source";
    }
}