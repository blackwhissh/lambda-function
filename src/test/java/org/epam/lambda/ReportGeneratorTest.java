package org.epam.lambda;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.StringWriter;
import java.time.LocalDate;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ReportGeneratorTest {

    @Mock
    private AmazonS3 mockS3Client;

    @InjectMocks
    private ReportGenerator reportGenerator;

    @Test
    void testSaveInS3() {
        StringWriter writer = new StringWriter();
        writer.write("Sample CSV Content");
        LocalDate now = LocalDate.now();

        ReportGenerator.saveInS3(writer, now, mockS3Client);

        verify(mockS3Client).putObject(any(PutObjectRequest.class));
    }
}
